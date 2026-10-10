import json
import logging
import os
import sys
import time
import uuid
from datetime import datetime, timezone

import pandas as pd
from kafka import KafkaProducer
from pydantic import BaseModel, ConfigDict, Field, ValidationError

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "transactions")
RATE = float(os.getenv("RATE_PER_SEC", "1"))
CSV_PATH = os.getenv("CSV_PATH", "/opt/data/test.csv")


def load_data():
    try:
        df = pd.read_csv(CSV_PATH)
        return df.drop(columns=["Class"], errors="ignore")
    except Exception:
        logger.exception("Erreur lors du chargement des données : %s", CSV_PATH)
        sys.exit(1)


def connect():
    while True:
        try:
            return KafkaProducer(
                bootstrap_servers=BOOTSTRAP,
                client_id="t_producer_1",
                value_serializer=lambda tx: tx.model_dump_json().encode("utf-8"),
            )
        except Exception:
            logger.error("Kafka pas prêt, nouvelle tentative...")
            time.sleep(3)


class Transaction(BaseModel):
    model_config = ConfigDict(extra="ignore")  # ignore les colonnes en trop (ex. Class)

    transaction_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    ts: str = Field(default_factory=lambda: datetime.now(timezone.utc).isoformat())

    Time: float
    V1: float
    V2: float
    V3: float
    V4: float
    V5: float
    V6: float
    V7: float
    V8: float
    V9: float
    V10: float
    V11: float
    V12: float
    V13: float
    V14: float
    V15: float
    V16: float
    V17: float
    V18: float
    V19: float
    V20: float
    V21: float
    V22: float
    V23: float
    V24: float
    V25: float
    V26: float
    V27: float
    V28: float
    Amount: float = Field(ge=0)  # un montant ne peut pas être négatif

    @classmethod
    def from_row(cls, row) -> "Transaction":
        """Construit une Transaction depuis une ligne pandas (Series)."""
        return cls(**row.to_dict())


if __name__ == "__main__":
    txs = load_data()
    producer = connect()
    logger.info("Producer actif -> %s/%s (%d lignes)", BOOTSTRAP, TOPIC, len(txs))

    try:
        while True:
            for i, row in txs.iterrows():
                try:
                    tx = Transaction.from_row(row)
                except ValidationError as e:
                    logger.error("Ligne %d invalide : %s", i, e)
                    continue
                producer.send(TOPIC, tx)
                logger.info("Envoyé #%d Amount=%s", i, tx.Amount)
                time.sleep(1 / RATE)
    except KeyboardInterrupt:
        logger.info("Arrêt demandé")
    finally:
        producer.flush()
        producer.close()