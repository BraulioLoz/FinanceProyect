"""
streaming_inference.py — Structured Streaming: crypto-features → scoring → crypto-pred + Parquet

Carga el modelo entrenado por batch_train.py, consume crypto-features en streaming,
aplica el modelo a cada micro-batch y publica predicciones a crypto-pred y a Parquet.

Uso:
    bash infra/scripts/run_spark_cpu.sh spark/jobs/streaming_inference.py
    bash infra/scripts/run_spark_gpu.sh spark/jobs/streaming_inference.py

Prerequisito: correr batch_train.py al menos una vez para generar el modelo.
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

load_dotenv()

# ── Variables de entorno ───────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS: str = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC_FEATURES: str = os.getenv("KAFKA_TOPIC_FEATURES", "crypto-features")
KAFKA_TOPIC_PREDICTIONS: str = os.getenv("KAFKA_TOPIC_PREDICTIONS", "crypto-pred")
SPARK_CHECKPOINT_BASE: str = os.getenv("SPARK_CHECKPOINT_BASE", "/tmp/spark-checkpoints")
DATA_DIR: str = os.environ["DATA_DIR"]
MODEL_DIR: str = os.environ["MODEL_DIR"]

INFERENCE_CHECKPOINT_DIR: str = f"{SPARK_CHECKPOINT_BASE}/streaming_inference"
PREDICTIONS_PARQUET_DIR: str = f"{DATA_DIR}/predictions"
MODEL_INPUT_DIR: str = f"{MODEL_DIR}/rf_price_direction"

WATERMARK_DELAY: str = "10 seconds"

# ── Schema del envelope de features (output de streaming_features.py) ─────────

FEATURE_PAYLOAD_SCHEMA = StructType([
    StructField("vwap", DoubleType(), True),
    StructField("price_volatility", DoubleType(), True),
    StructField("total_volume", DoubleType(), True),
    StructField("trade_count", LongType(), True),
    StructField("avg_price", DoubleType(), True),
    StructField("avg_spread_proxy", DoubleType(), True),
    StructField("avg_best_bid_price", DoubleType(), True),
    StructField("avg_best_ask_price", DoubleType(), True),
])

FEATURE_ENVELOPE_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("exchange", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("ts_event", StringType(), False),
    StructField("ts_ingest", StringType(), True),
    StructField("payload", FEATURE_PAYLOAD_SCHEMA, True),
])

# Columnas de features que el modelo espera (deben coincidir con batch_train.py)
MODEL_FEATURE_COLUMNS: list[str] = [
    "vwap",
    "price_volatility",
    "total_volume",
    "trade_count",
    "avg_price",
    "avg_spread_proxy",
    "avg_best_bid_price",
    "avg_best_ask_price",
    # lag features — no disponibles en tiempo real; se rellenan con 0 para inferencia
    "vwap_lag_1",
    "price_volatility_lag_1",
    "total_volume_lag_1",
    "avg_spread_proxy_lag_1",
    "vwap_lag_2",
    "price_volatility_lag_2",
]


# ── Helpers ───────────────────────────────────────────────────────────────────

def _load_trained_model(model_input_dir: str) -> PipelineModel:
    print(f"Cargando modelo desde: {model_input_dir}")
    return PipelineModel.load(model_input_dir)


def _read_features_stream(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", KAFKA_TOPIC_FEATURES)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) AS raw_json")
        .select(
            F.from_json(F.col("raw_json"), FEATURE_ENVELOPE_SCHEMA).alias("envelope")
        )
        .select(
            F.col("envelope.symbol").alias("symbol"),
            F.to_timestamp(F.col("envelope.ts_event")).alias("ts_event_timestamp"),
            F.col("envelope.ts_event").alias("ts_event"),
            F.col("envelope.payload.vwap").alias("vwap"),
            F.col("envelope.payload.price_volatility").alias("price_volatility"),
            F.col("envelope.payload.total_volume").alias("total_volume"),
            F.col("envelope.payload.trade_count").alias("trade_count"),
            F.col("envelope.payload.avg_price").alias("avg_price"),
            F.col("envelope.payload.avg_spread_proxy").alias("avg_spread_proxy"),
            F.col("envelope.payload.avg_best_bid_price").alias("avg_best_bid_price"),
            F.col("envelope.payload.avg_best_ask_price").alias("avg_best_ask_price"),
        )
        .withWatermark("ts_event_timestamp", WATERMARK_DELAY)
        # Lag features no disponibles en tiempo real → se rellenan con 0
        .withColumn("vwap_lag_1", F.lit(0.0))
        .withColumn("price_volatility_lag_1", F.lit(0.0))
        .withColumn("total_volume_lag_1", F.lit(0.0))
        .withColumn("avg_spread_proxy_lag_1", F.lit(0.0))
        .withColumn("vwap_lag_2", F.lit(0.0))
        .withColumn("price_volatility_lag_2", F.lit(0.0))
    )


def _build_prediction_envelope(
    symbol: str,
    ts_event: str,
    prediction: int,
    probability_up: float,
    probability_down: float,
) -> str:
    ingest_timestamp_utc = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    envelope = {
        "symbol": symbol,
        "exchange": "binance_futures",
        "event_type": "prediction",
        "ts_event": ts_event,
        "ts_ingest": ingest_timestamp_utc,
        "payload": {
            "price_direction_prediction": int(prediction),
            "probability_price_up": round(float(probability_up), 6),
            "probability_price_down": round(float(probability_down), 6),
            "model_name": "rf_price_direction",
        },
    }
    return json.dumps(envelope, ensure_ascii=False)


def _write_predictions_batch_to_kafka_and_parquet(
    predictions_micro_batch_df: DataFrame,
    batch_id: int,
    kafka_topic_predictions: str,
    predictions_parquet_dir: str,
) -> None:
    if predictions_micro_batch_df.isEmpty():
        return

    prediction_envelope_udf = F.udf(
        lambda symbol, ts_event, prediction, probability: (
            _build_prediction_envelope(
                symbol,
                ts_event or "",
                int(prediction) if prediction is not None else -1,
                float(probability[1]) if probability is not None and len(probability) > 1 else 0.0,
                float(probability[0]) if probability is not None else 0.0,
            )
        ),
        StringType(),
    )

    kafka_ready_df = predictions_micro_batch_df.withColumn(
        "value",
        prediction_envelope_udf(
            F.col("symbol"),
            F.col("ts_event"),
            F.col("prediction").cast(IntegerType()),
            F.col("probability"),
        ),
    )

    # Publicar a Kafka
    (
        kafka_ready_df.select(
            F.col("symbol").alias("key"),
            F.col("value"),
        )
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", kafka_topic_predictions)
        .save()
    )

    # Guardar en Parquet
    (
        predictions_micro_batch_df
        .withColumn("date", F.to_date(F.col("ts_event_timestamp")))
        .select(
            "symbol", "ts_event", "ts_event_timestamp", "date",
            "prediction", "probability",
            "vwap", "price_volatility", "total_volume",
        )
        .write
        .mode("append")
        .partitionBy("date", "symbol")
        .parquet(predictions_parquet_dir)
    )


def run_streaming_inference(spark: SparkSession) -> None:
    trained_pipeline_model = _load_trained_model(MODEL_INPUT_DIR)

    features_stream_df = _read_features_stream(spark)

    # El modelo espera un DataFrame estático — se aplica en foreachBatch
    def score_and_write_micro_batch(micro_batch_df: DataFrame, batch_id: int) -> None:
        if micro_batch_df.isEmpty():
            return

        predictions_df = trained_pipeline_model.transform(micro_batch_df)
        _write_predictions_batch_to_kafka_and_parquet(
            predictions_df,
            batch_id,
            KAFKA_TOPIC_PREDICTIONS,
            PREDICTIONS_PARQUET_DIR,
        )

    query = (
        features_stream_df
        .writeStream
        .outputMode("append")
        .option("checkpointLocation", INFERENCE_CHECKPOINT_DIR)
        .foreachBatch(score_and_write_micro_batch)
        .start()
    )

    print(f"Streaming inference iniciado — checkpoint: {INFERENCE_CHECKPOINT_DIR}")
    print(f"Predicciones Parquet: {PREDICTIONS_PARQUET_DIR}")
    print(f"Topic de salida: {KAFKA_TOPIC_PREDICTIONS}")
    print("Spark UI: http://localhost:4040")
    query.awaitTermination()


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("crypto-streaming-inference")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    run_streaming_inference(spark)
