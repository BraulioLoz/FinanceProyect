"""
streaming_features.py — Structured Streaming: crypto-trades + crypto-book → crypto-features + Parquet

Lee los topics de ingesta, calcula features por ventana de tiempo (VWAP, volatilidad,
volumen, spread proxy) y escribe los resultados a Kafka y a Parquet local.

Uso:
    bash infra/scripts/run_spark_cpu.sh spark/jobs/streaming_features.py
    bash infra/scripts/run_spark_gpu.sh spark/jobs/streaming_features.py
"""
from __future__ import annotations

import json
import os
from datetime import datetime, timezone

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    BooleanType,
    DoubleType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

load_dotenv()

# ── Variables de entorno ───────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS: str = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC_TRADES: str = os.getenv("KAFKA_TOPIC_TRADES", "crypto-trades")
KAFKA_TOPIC_BOOK: str = os.getenv("KAFKA_TOPIC_BOOK", "crypto-book")
KAFKA_TOPIC_FEATURES: str = os.getenv("KAFKA_TOPIC_FEATURES", "crypto-features")
SPARK_CHECKPOINT_BASE: str = os.getenv("SPARK_CHECKPOINT_BASE", "/tmp/spark-checkpoints")
DATA_DIR: str = os.environ["DATA_DIR"]

FEATURES_CHECKPOINT_DIR: str = f"{SPARK_CHECKPOINT_BASE}/streaming_features"
FEATURES_PARQUET_DIR: str = f"{DATA_DIR}/features"

# Watermark tolerance: descarta eventos que lleguen más de N segundos tarde
WATERMARK_DELAY: str = "10 seconds"

# Ventana principal para features de entrenamiento (1 min tamaño, 30 s slide)
WINDOW_DURATION: str = "1 minute"
WINDOW_SLIDE: str = "30 seconds"

# Ventana corta para features de anomalías en tiempo real
SHORT_WINDOW_DURATION: str = "5 seconds"
SHORT_WINDOW_SLIDE: str = "5 seconds"

# ── Schemas explícitos del envelope ───────────────────────────────────────────

TRADE_PAYLOAD_SCHEMA = StructType([
    StructField("aggregate_trade_id", LongType(), True),
    StructField("price", StringType(), True),
    StructField("quantity", StringType(), True),
    StructField("quantity_excluding_rpi", StringType(), True),
    StructField("first_trade_id", LongType(), True),
    StructField("last_trade_id", LongType(), True),
    StructField("trade_execution_time_ms", LongType(), True),
    StructField("is_buyer_market_maker", BooleanType(), True),
])

ENVELOPE_WITH_TRADE_PAYLOAD_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("exchange", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("ts_event", StringType(), False),
    StructField("ts_ingest", StringType(), True),
    StructField("payload", TRADE_PAYLOAD_SCHEMA, True),
])

DEPTH_LEVEL_SCHEMA = StructType([
    StructField("price", StringType(), True),
    StructField("quantity", StringType(), True),
])

BOOK_PAYLOAD_SCHEMA = StructType([
    StructField("first_update_id", LongType(), True),
    StructField("final_update_id", LongType(), True),
    StructField("previous_final_update_id", LongType(), True),
    StructField("bids", StringType(), True),   # JSON array — se parsea en UDF
    StructField("asks", StringType(), True),   # JSON array — se parsea en UDF
])

ENVELOPE_WITH_BOOK_PAYLOAD_SCHEMA = StructType([
    StructField("symbol", StringType(), False),
    StructField("exchange", StringType(), True),
    StructField("event_type", StringType(), True),
    StructField("ts_event", StringType(), False),
    StructField("ts_ingest", StringType(), True),
    StructField("payload", BOOK_PAYLOAD_SCHEMA, True),
])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _read_kafka_stream(spark: SparkSession, topic: str) -> DataFrame:
    return (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
        .selectExpr("CAST(value AS STRING) AS raw_json")
    )


def _parse_trade_stream(raw_stream: DataFrame) -> DataFrame:
    parsed = raw_stream.select(
        F.from_json(F.col("raw_json"), ENVELOPE_WITH_TRADE_PAYLOAD_SCHEMA).alias("envelope")
    ).select(
        F.col("envelope.symbol").alias("symbol"),
        F.to_timestamp(F.col("envelope.ts_event")).alias("ts_event_timestamp"),
        F.col("envelope.payload.price").cast(DoubleType()).alias("price"),
        F.col("envelope.payload.quantity").cast(DoubleType()).alias("quantity"),
        F.col("envelope.payload.is_buyer_market_maker").alias("is_buyer_market_maker"),
    )
    return parsed.withWatermark("ts_event_timestamp", WATERMARK_DELAY)


def _parse_book_stream(raw_stream: DataFrame) -> DataFrame:
    """
    Extrae el mejor bid y mejor ask de cada snapshot de libro.
    Los arrays b/a del envelope llegan como strings JSON; se expanden con from_json.
    """
    bid_ask_schema = StructType([
        StructField("price", StringType(), True),
        StructField("quantity", StringType(), True),
    ])
    array_schema = StructType([
        StructField("best_bid_price", StringType(), True),
        StructField("best_ask_price", StringType(), True),
    ])

    parsed = raw_stream.select(
        F.from_json(F.col("raw_json"), ENVELOPE_WITH_BOOK_PAYLOAD_SCHEMA).alias("envelope")
    ).select(
        F.col("envelope.symbol").alias("symbol"),
        F.to_timestamp(F.col("envelope.ts_event")).alias("ts_event_timestamp"),
        # Tomar el primer nivel de bids (mejor bid) y primer nivel de asks (mejor ask)
        F.get_json_object(F.col("envelope.payload.bids"), "$[0][0]").cast(DoubleType()).alias("best_bid_price"),
        F.get_json_object(F.col("envelope.payload.asks"), "$[0][0]").cast(DoubleType()).alias("best_ask_price"),
    )
    return parsed.withWatermark("ts_event_timestamp", WATERMARK_DELAY)


def _compute_trade_features(trade_stream: DataFrame, window_duration: str, window_slide: str) -> DataFrame:
    """VWAP, volatilidad, volumen y trade count por ventana y símbolo."""
    return (
        trade_stream
        .groupBy(
            F.col("symbol"),
            F.window(F.col("ts_event_timestamp"), window_duration, window_slide),
        )
        .agg(
            (F.sum(F.col("price") * F.col("quantity")) / F.sum("quantity")).alias("vwap"),
            F.stddev("price").alias("price_volatility"),
            F.sum("quantity").alias("total_volume"),
            F.count("*").alias("trade_count"),
            F.avg("price").alias("avg_price"),
            F.min("price").alias("min_price"),
            F.max("price").alias("max_price"),
        )
        .select(
            F.col("symbol"),
            F.col("window.start").alias("window_start"),
            F.col("window.end").alias("window_end"),
            F.col("vwap"),
            F.col("price_volatility"),
            F.col("total_volume"),
            F.col("trade_count"),
            F.col("avg_price"),
            F.col("min_price"),
            F.col("max_price"),
        )
    )


def _compute_book_features(book_stream: DataFrame, window_duration: str, window_slide: str) -> DataFrame:
    """Spread proxy (mejor ask - mejor bid) promedio por ventana y símbolo."""
    return (
        book_stream
        .withColumn("spread_proxy", F.col("best_ask_price") - F.col("best_bid_price"))
        .groupBy(
            F.col("symbol"),
            F.window(F.col("ts_event_timestamp"), window_duration, window_slide),
        )
        .agg(
            F.avg("spread_proxy").alias("avg_spread_proxy"),
            F.avg("best_bid_price").alias("avg_best_bid_price"),
            F.avg("best_ask_price").alias("avg_best_ask_price"),
        )
        .select(
            F.col("symbol"),
            F.col("window.start").alias("window_start"),
            F.col("avg_spread_proxy"),
            F.col("avg_best_bid_price"),
            F.col("avg_best_ask_price"),
        )
    )


def _build_feature_envelope(row_symbol: str, window_start_iso: str, payload: dict) -> str:
    """Serializa una fila de features al envelope JSON estándar."""
    ingest_timestamp_utc = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    envelope = {
        "symbol": row_symbol,
        "exchange": "binance_futures",
        "event_type": "feature_row",
        "ts_event": window_start_iso,
        "ts_ingest": ingest_timestamp_utc,
        "payload": payload,
    }
    return json.dumps(envelope, ensure_ascii=False)


def _write_batch_to_kafka_and_parquet(
    micro_batch_df: DataFrame,
    batch_id: int,
    kafka_topic_features: str,
    parquet_output_dir: str,
) -> None:
    """foreachBatch handler: escribe cada micro-batch a Kafka y a Parquet."""
    if micro_batch_df.isEmpty():
        return

    # Serializar cada fila al envelope JSON
    feature_envelope_udf = F.udf(
        lambda symbol, window_start, vwap, price_volatility, total_volume, trade_count,
               avg_price, min_price, max_price, avg_spread_proxy, avg_best_bid_price, avg_best_ask_price: (
            _build_feature_envelope(
                symbol,
                window_start.isoformat() if window_start else "",
                {
                    "vwap": vwap,
                    "price_volatility": price_volatility,
                    "total_volume": total_volume,
                    "trade_count": trade_count,
                    "avg_price": avg_price,
                    "min_price": min_price,
                    "max_price": max_price,
                    "avg_spread_proxy": avg_spread_proxy,
                    "avg_best_bid_price": avg_best_bid_price,
                    "avg_best_ask_price": avg_best_ask_price,
                },
            )
        ),
        StringType(),
    )

    kafka_ready_df = micro_batch_df.withColumn(
        "value",
        feature_envelope_udf(
            F.col("symbol"),
            F.col("window_start"),
            F.col("vwap"),
            F.col("price_volatility"),
            F.col("total_volume"),
            F.col("trade_count"),
            F.col("avg_price"),
            F.col("min_price"),
            F.col("max_price"),
            F.col("avg_spread_proxy"),
            F.col("avg_best_bid_price"),
            F.col("avg_best_ask_price"),
        ),
    ).withColumn("key", F.col("symbol"))

    # Escribir a Kafka
    (
        kafka_ready_df.select(
            F.col("key").cast("string"),
            F.col("value").cast("string"),
        )
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", kafka_topic_features)
        .save()
    )

    # Escribir a Parquet particionado por fecha y símbolo
    (
        micro_batch_df
        .withColumn("date", F.to_date(F.col("window_start")))
        .write
        .mode("append")
        .partitionBy("date", "symbol")
        .parquet(parquet_output_dir)
    )


def run_streaming_features(spark: SparkSession) -> None:
    trade_raw_stream = _read_kafka_stream(spark, KAFKA_TOPIC_TRADES)
    book_raw_stream = _read_kafka_stream(spark, KAFKA_TOPIC_BOOK)

    trade_stream = _parse_trade_stream(trade_raw_stream)
    book_stream = _parse_book_stream(book_raw_stream)

    trade_features_df = _compute_trade_features(trade_stream, WINDOW_DURATION, WINDOW_SLIDE)
    book_features_df = _compute_book_features(book_stream, WINDOW_DURATION, WINDOW_SLIDE)

    # Join inner por símbolo y ventana — stream-stream outer join requiere
    # condición de rango temporal explícita en Spark; inner es correcto aquí
    # porque ambos streams producen datos para las mismas ventanas.
    combined_features_df = (
        trade_features_df
        .join(
            book_features_df,
            on=["symbol", "window_start"],
            how="inner",
        )
    )

    query = (
        combined_features_df
        .writeStream
        .outputMode("append")
        .option("checkpointLocation", FEATURES_CHECKPOINT_DIR)
        .foreachBatch(
            lambda micro_batch_df, batch_id: _write_batch_to_kafka_and_parquet(
                micro_batch_df,
                batch_id,
                KAFKA_TOPIC_FEATURES,
                FEATURES_PARQUET_DIR,
            )
        )
        .start()
    )

    print(f"Streaming features iniciado — checkpoint: {FEATURES_CHECKPOINT_DIR}")
    print(f"Parquet output: {FEATURES_PARQUET_DIR}")
    print("Spark UI: http://localhost:4040")
    query.awaitTermination()


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("crypto-streaming-features")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    run_streaming_features(spark)
