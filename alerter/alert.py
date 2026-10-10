import json
import logging
import os
import smtplib
import ssl
import time
from email.message import EmailMessage

from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(levelname)s - %(message)s",
)
logger = logging.getLogger("alerter")

# ---------------------------------------------------------------- config
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
TOPIC = os.getenv("TOPIC", "predictions")

SMTP_HOST = os.getenv("SMTP_HOST", "mailpit")
SMTP_PORT = int(os.getenv("SMTP_PORT", "1025"))
SMTP_USER = os.getenv("SMTP_USER", "")
SMTP_PASSWORD = os.getenv("SMTP_PASSWORD", "")
SMTP_STARTTLS = os.getenv("SMTP_STARTTLS", "false").lower() == "true"

MAIL_FROM = os.getenv("MAIL_FROM", "fraud-alert@example.com")
MAIL_TO = [a.strip() for a in os.getenv("MAIL_TO", "analyste@example.com").split(",") if a.strip()]

# Anti-spam : au plus 1 email toutes les N secondes (les fraudes sont regroupées)
MIN_INTERVAL = float(os.getenv("ALERT_MIN_INTERVAL", "10"))
MIN_SCORE = float(os.getenv("ALERT_MIN_SCORE", "0.0"))
MAX_BUFFER = 500


# ---------------------------------------------------------------- email
def build_message(frauds):
    n = len(frauds)
    msg = EmailMessage()
    msg["From"] = MAIL_FROM
    msg["To"] = ", ".join(MAIL_TO)
    msg["Subject"] = (
        f"[ALERTE FRAUDE] {n} transaction suspecte" if n == 1
        else f"[ALERTE FRAUDE] {n} transactions suspectes"
    )

    lines = [f"{n} transaction(s) détectée(s) comme fraude :", ""]
    for r in frauds[:50]:
        score = r.get("fraud_score")
        amount = r.get("amount")
        lines.append(
            f"- ID : {r.get('transaction_id')}\n"
            f"  Montant : {amount if amount is not None else 'inconnu'}\n"
            f"  Score de fraude : {score:.4f}\n"
            if isinstance(score, (int, float)) else
            f"- ID : {r.get('transaction_id')} (score inconnu)\n"
        )
    if n > 50:
        lines.append(f"... et {n - 50} autre(s).")
    lines.append("\nConsulte le dashboard pour investiguer.")
    msg.set_content("\n".join(lines))
    return msg


def send_email(frauds):
    msg = build_message(frauds)
    if SMTP_PORT == 465:
        server = smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=15,
                                  context=ssl.create_default_context())
    else:
        server = smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15)
    with server:
        if SMTP_STARTTLS and SMTP_PORT != 465:
            server.starttls(context=ssl.create_default_context())
        if SMTP_USER:
            server.login(SMTP_USER, SMTP_PASSWORD)
        server.send_message(msg)
    logger.info("Email envoyé à %s (%d fraude(s))", MAIL_TO, len(frauds))


# ---------------------------------------------------------------- Kafka
def parse(raw):
    try:
        return json.loads(raw.decode())
    except Exception:
        logger.warning("Message ignoré (JSON invalide) : %r", raw[:200])
        return None


def connect_kafka():
    while True:
        try:
            consumer = KafkaConsumer(
                TOPIC,
                bootstrap_servers=BOOTSTRAP,
                group_id="alerter",
                auto_offset_reset="latest",  # pas d'email pour l'historique
                value_deserializer=parse,
            )
            logger.info("Connecté à Kafka topic=%s", TOPIC)
            return consumer
        except NoBrokersAvailable:
            logger.warning("Kafka indisponible, nouvel essai dans 3s")
            time.sleep(3)


def main():
    logger.info("SMTP=%s:%s (starttls=%s) -> %s", SMTP_HOST, SMTP_PORT, SMTP_STARTTLS, MAIL_TO)
    consumer = connect_kafka()
    logger.info("alerter actif")

    buffer = []
    last_sent = 0.0

    while True:
        batches = consumer.poll(timeout_ms=1000)
        for records in batches.values():
            for msg in records:
                r = msg.value
                if not r or r.get("is_fraud") != 1:
                    continue
                if (r.get("fraud_score") or 0) < MIN_SCORE:
                    continue
                logger.warning("FRAUDE tx=%s score=%s amount=%s",
                               r.get("transaction_id"), r.get("fraud_score"), r.get("amount"))
                buffer.append(r)

        if len(buffer) > MAX_BUFFER:
            logger.error("Buffer plein, %d alertes les plus anciennes abandonnées",
                         len(buffer) - MAX_BUFFER)
            buffer = buffer[-MAX_BUFFER:]

        if buffer and time.time() - last_sent >= MIN_INTERVAL:
            try:
                send_email(buffer)
                buffer = []
            except Exception:
                logger.exception("Échec d'envoi de l'email, nouvel essai dans %.0fs", MIN_INTERVAL)
            last_sent = time.time()


if __name__ == "__main__":
    main()