import json
import logging
import os
import time

import psycopg2
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("db-writer")

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "predictions")

INSERT_SQL = """
    INSERT INTO transactions_scored
        (transaction_id, tx_time, amount, fraud_score, is_fraud)
    VALUES (%s, %s, %s, %s, %s)
    ON CONFLICT (transaction_id) DO NOTHING
"""


def connect_pg():
    while True:
        try:
            conn = psycopg2.connect(
                host=os.getenv("PG_HOST", "postgres"),
                dbname="fraud", user="fraud", password="fraud",
            )
            conn.autocommit = True  # chaque INSERT est validé tout seul
            logger.info("Connecté à PostgreSQL")
            return conn
        except psycopg2.OperationalError as e:
            logger.warning("PostgreSQL indisponible (%s), nouvel essai dans 3s", e)
            time.sleep(3)


def connect_kafka():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP,
                group_id="db-writer",
                auto_offset_reset="earliest",
                value_deserializer=lambda v: json.loads(v.decode()),
            )
            logger.info("Connecté à Kafka topic=%s", TOPIC)
            return consumer
        except NoBrokersAvailable:
            logger.warning("Kafka indisponible, nouvel essai dans 3s")
            time.sleep(3)


conn = connect_pg()
cur = conn.cursor()
consumer = connect_kafka()
logger.info("db-writer actif")

for msg in consumer:
    r = msg.value
    tx_id = r.get("transaction_id")
    try:
        cur.execute(
            INSERT_SQL,
            (tx_id, r.get("tx_time"), r.get("amount"),
             r.get("fraud_score"), r.get("is_fraud")),
        )
        if cur.rowcount == 1:
            logger.info("Inserted tx=%s score=%s fraud=%s",
                        tx_id, r.get("fraud_score"), r.get("is_fraud"))
        else:
            logger.warning("Duplicate ignored tx=%s", tx_id)

    except (psycopg2.OperationalError, psycopg2.InterfaceError):
        # connexion perdue : on se reconnecte (le message sera rejoué si besoin)
        logger.exception("Connexion PostgreSQL perdue tx=%s", tx_id)
        conn = connect_pg()
        cur = conn.cursor()

    except Exception:
        logger.exception("Insert failed tx=%s payload=%s", tx_id, r)