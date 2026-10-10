import logging
import os
import sys
import time

import joblib
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, pandas_udf, struct, to_json
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

# ---------------------------------------------------------------- logging
logging.basicConfig(
    level=os.getenv("LOG_LEVEL", "INFO"),
    format="%(asctime)s - %(levelname)s - %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("stream-inference")

# ---------------------------------------------------------------- config
BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "kafka:9092")
INPUT_TOPIC = "transactions"
OUTPUT_TOPIC = "predictions"
MODEL_DIR = "/models/final_model_xgb_ros.pkl"
THRESHOLD = float(os.getenv("FRAUD_THRESHOLD", "0.5"))
STARTING_OFFSETS = os.getenv("STARTING_OFFSETS", "earliest")
SPARK_LOG_LEVEL = os.getenv("SPARK_LOG_LEVEL", "WARN")

# Mêmes colonnes, dans le même ordre qu'à l'entraînement
FEATURES = [
    "Time",
    "V1", "V2", "V3", "V4", "V5", "V6", "V7",
    "V8", "V9", "V10", "V11", "V12", "V13", "V14",
    "V15", "V16", "V17", "V18", "V19", "V20", "V21",
    "V22", "V23", "V24", "V25", "V26", "V27", "V28",
    "Amount",
]

META = ["transaction_id"]

schema = StructType(
    [StructField(m, StringType()) for m in META]
    + [StructField(f, DoubleType()) for f in FEATURES]
)

logger.info(
    "Config: bootstrap=%s in=%s out=%s threshold=%s startingOffsets=%s",
    BOOTSTRAP, INPUT_TOPIC, OUTPUT_TOPIC, THRESHOLD, STARTING_OFFSETS,
)

# ---------------------------------------------------------------- Spark
spark = (
    SparkSession.builder
    .master("local[1]")
    .appName("fraud-inference")
    .config("spark.sql.shuffle.partitions", "2")
    .getOrCreate()
)
spark.sparkContext.setLogLevel(SPARK_LOG_LEVEL)
logger.info("SparkSession démarrée (Spark %s)", spark.version)

# ---------------------------------------------------------------- modèle
try:
    t0 = time.time()
    model = joblib.load(MODEL_DIR)
    logger.info(
        "Modèle chargé depuis %s (%s) en %.1f s",
        MODEL_DIR, type(model).__name__, time.time() - t0,
    )
except Exception:
    logger.exception("Impossible de charger le modèle depuis %s", MODEL_DIR)
    sys.exit(1)

bc_model = spark.sparkContext.broadcast(model)


# ---------------------------------------------------------------- UDF
@pandas_udf("double")
def predict(*cols: pd.Series) -> pd.Series:
    t0 = time.time()
    X = pd.concat(cols, axis=1)
    X.columns = FEATURES
    scores = pd.Series(bc_model.value.predict_proba(X)[:, 1])

    # Le UDF tourne dans un process Python séparé : on configure son logger ici
    log = logging.getLogger("udf")
    if not log.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(
            logging.Formatter("%(asctime)s - %(levelname)s - [udf] %(message)s")
        )
        log.addHandler(handler)
        log.setLevel(logging.INFO)
        log.propagate = False

    log.info(
        "Scoré %d lignes en %.0f ms | score max=%.3f | au-dessus du seuil=%d",
        len(X), (time.time() - t0) * 1000, scores.max(),
        int((scores > THRESHOLD).sum()),
    )
    return scores


# ---------------------------------------------------------------- listener
class ProgressLogger(StreamingQueryListener):
    def onQueryStarted(self, event):
        logger.info("Requête streaming démarrée id=%s", event.id)

    def onQueryProgress(self, event):
        p = event.progress
        if p.numInputRows > 0:  # on ignore les batchs vides
            logger.info(
                "Batch %d | %d lignes | in=%.1f/s traité=%.1f/s | durée=%s ms",
                p.batchId, p.numInputRows,
                p.inputRowsPerSecond or 0.0, p.processedRowsPerSecond or 0.0,
                p.durationMs.get("triggerExecution"),
            )

    def onQueryIdle(self, event):
        pass

    def onQueryTerminated(self, event):
        if event.exception:
            logger.error("Requête arrêtée avec erreur : %s", event.exception)
        else:
            logger.info("Requête arrêtée proprement id=%s", event.id)


spark.streams.addListener(ProgressLogger())

# ---------------------------------------------------------------- lecture Kafka
raw = (
    spark.readStream.format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("subscribe", INPUT_TOPIC)
    .option("startingOffsets", STARTING_OFFSETS)
    .load()
)

# Parsing du JSON
parsed = (
    raw.select(from_json(col("value").cast("string"), schema).alias("d"))
    .where(col("d").isNotNull())
    .select("d.*")
    .dropna(subset=FEATURES)
)

# Prédiction
scored = (
    parsed
    .withColumn("fraud_score", predict(*[col(f) for f in FEATURES]))
    .withColumn("is_fraud", (col("fraud_score") > THRESHOLD).cast("int"))
)

# ---------------------------------------------------------------- écriture Kafka
query = (
    scored.select(
        to_json(struct(
            col("transaction_id"),
            col("Time").alias("tx_time"),
            col("Amount").alias("amount"),
            col("fraud_score"),
            col("is_fraud"),
        )).alias("value")
    )
    .writeStream
    .format("kafka")
    .option("kafka.bootstrap.servers", BOOTSTRAP)
    .option("topic", OUTPUT_TOPIC)
    .option("checkpointLocation", "/tmp/checkpoints/kafka")
    .outputMode("append")
    .start()
)
logger.info("Streaming actif : %s -> %s", INPUT_TOPIC, OUTPUT_TOPIC)

try:
    query.awaitTermination()
except KeyboardInterrupt:
    logger.info("Arrêt demandé")
    query.stop()
except Exception:
    logger.exception("Le streaming a planté")
    raise