"""
replay_parquet.py — Replay de Parquet histórico → Kafka

Lee features o trades almacenados en Parquet e inyecta los mensajes a Kafka
a velocidad configurable, permitiendo reproducir exactamente la misma carga
para comparar CPU vs GPU con datos idénticos.

Uso:
    python spark/replayer/replay_parquet.py --source features --speed 1.0
    python spark/replayer/replay_parquet.py --source trades   --speed 2.0
    python spark/replayer/replay_parquet.py --source features --speed 0.0  # máxima velocidad

Argumentos:
    --source   features | trades | predictions  (default: features)
    --speed    factor de velocidad respecto al tiempo real (1.0 = tiempo real,
               2.0 = doble de rápido, 0.0 = sin throttle)
    --symbol   filtrar por símbolo (default: todos)
    --date     filtrar por fecha ISO (default: todos)
    --dry-run  parsear y contar mensajes sin publicar a Kafka
"""
from __future__ import annotations

import argparse
import json
import os
import time
from datetime import datetime, timezone
from typing import Iterator

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StringType

load_dotenv()

# ── Variables de entorno ───────────────────────────────────────────────────────

KAFKA_BOOTSTRAP_SERVERS: str = os.environ["KAFKA_BOOTSTRAP_SERVERS"]
KAFKA_TOPIC_FEATURES: str = os.getenv("KAFKA_TOPIC_FEATURES", "crypto-features")
KAFKA_TOPIC_TRADES: str = os.getenv("KAFKA_TOPIC_TRADES", "crypto-trades")
KAFKA_TOPIC_PREDICTIONS: str = os.getenv("KAFKA_TOPIC_PREDICTIONS", "crypto-pred")
DATA_DIR: str = os.environ["DATA_DIR"]

SOURCE_PARQUET_DIRS: dict[str, str] = {
    "features": f"{DATA_DIR}/features",
    "trades": f"{DATA_DIR}/trades_raw",
    "predictions": f"{DATA_DIR}/predictions",
}

SOURCE_KAFKA_TOPICS: dict[str, str] = {
    "features": KAFKA_TOPIC_FEATURES,
    "trades": KAFKA_TOPIC_TRADES,
    "predictions": KAFKA_TOPIC_PREDICTIONS,
}

# ── Builders de envelope para replay ─────────────────────────────────────────

def _build_replay_feature_envelope(row) -> str:
    ingest_timestamp_utc = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    payload = {
        "vwap": row.vwap,
        "price_volatility": row.price_volatility,
        "total_volume": row.total_volume,
        "trade_count": int(row.trade_count),
        "avg_price": row.avg_price,
        "avg_spread_proxy": row.avg_spread_proxy,
        "avg_best_bid_price": row.avg_best_bid_price,
        "avg_best_ask_price": row.avg_best_ask_price,
        "_replayed": True,
    }
    envelope = {
        "symbol": row.symbol,
        "exchange": "binance_futures",
        "event_type": "feature_row",
        "ts_event": row.window_start.isoformat() if row.window_start else ingest_timestamp_utc,
        "ts_ingest": ingest_timestamp_utc,
        "payload": payload,
    }
    return json.dumps(envelope, ensure_ascii=False)


def _build_replay_prediction_envelope(row) -> str:
    ingest_timestamp_utc = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    payload = {
        "price_direction_prediction": int(row.prediction),
        "vwap": row.vwap,
        "_replayed": True,
    }
    envelope = {
        "symbol": row.symbol,
        "exchange": "binance_futures",
        "event_type": "prediction",
        "ts_event": row.ts_event if row.ts_event else ingest_timestamp_utc,
        "ts_ingest": ingest_timestamp_utc,
        "payload": payload,
    }
    return json.dumps(envelope, ensure_ascii=False)


ENVELOPE_BUILDERS: dict[str, callable] = {
    "features": _build_replay_feature_envelope,
    "predictions": _build_replay_prediction_envelope,
}

# ── Carga y filtrado ───────────────────────────────────────────────────────────

def _load_parquet(
    spark: SparkSession,
    source: str,
    symbol_filter: str | None,
    date_filter: str | None,
) -> DataFrame:
    parquet_dir = SOURCE_PARQUET_DIRS[source]
    print(f"Leyendo Parquet desde: {parquet_dir}")

    df = spark.read.parquet(parquet_dir)

    if symbol_filter:
        df = df.filter(F.col("symbol") == symbol_filter.upper())
        print(f"Filtro símbolo: {symbol_filter.upper()}")

    if date_filter:
        df = df.filter(F.col("date") == date_filter)
        print(f"Filtro fecha: {date_filter}")

    return df


def _get_sort_column(source: str) -> str:
    sort_columns: dict[str, str] = {
        "features": "window_start",
        "trades": "ts_event",
        "predictions": "ts_event_timestamp",
    }
    return sort_columns.get(source, "window_start")


# ── Publicación con throttle ───────────────────────────────────────────────────

def _iter_rows_with_throttle(
    df: DataFrame,
    source: str,
    replay_speed_factor: float,
) -> Iterator[tuple[str, str]]:
    """
    Itera las filas ordenadas por timestamp y aplica throttle para reproducir
    el timing original escalado por replay_speed_factor.

    replay_speed_factor=1.0 → tiempo real
    replay_speed_factor=2.0 → el doble de rápido
    replay_speed_factor=0.0 → sin throttle (máxima velocidad)
    """
    sort_column = _get_sort_column(source)
    envelope_builder = ENVELOPE_BUILDERS.get(source)
    rows = df.orderBy(sort_column).collect()

    previous_event_timestamp: datetime | None = None
    previous_wall_clock_time: float | None = None

    for row in rows:
        # Obtener timestamp del evento para calcular el gap con el anterior
        raw_event_time = getattr(row, sort_column, None)
        current_event_timestamp: datetime | None = None

        if raw_event_time is not None:
            if isinstance(raw_event_time, datetime):
                current_event_timestamp = raw_event_time
            else:
                try:
                    current_event_timestamp = datetime.fromisoformat(str(raw_event_time))
                except (ValueError, TypeError):
                    current_event_timestamp = None

        # Throttle basado en el gap entre eventos originales
        if (
            replay_speed_factor > 0.0
            and previous_event_timestamp is not None
            and current_event_timestamp is not None
            and previous_wall_clock_time is not None
        ):
            original_gap_seconds = (
                current_event_timestamp - previous_event_timestamp
            ).total_seconds()

            if original_gap_seconds > 0:
                target_wall_clock_gap_seconds = original_gap_seconds / replay_speed_factor
                elapsed_wall_clock_seconds = time.monotonic() - previous_wall_clock_time
                sleep_duration_seconds = target_wall_clock_gap_seconds - elapsed_wall_clock_seconds

                if sleep_duration_seconds > 0:
                    time.sleep(sleep_duration_seconds)

        previous_event_timestamp = current_event_timestamp
        previous_wall_clock_time = time.monotonic()

        # Construir envelope
        if envelope_builder is not None:
            envelope_json = envelope_builder(row)
        else:
            # Para fuentes sin builder explícito, serializar todas las columnas como payload
            envelope_json = json.dumps({
                "symbol": getattr(row, "symbol", "UNKNOWN"),
                "exchange": "binance_futures",
                "event_type": f"{source}_replay",
                "ts_event": str(raw_event_time) if raw_event_time else "",
                "ts_ingest": datetime.now(timezone.utc).isoformat(timespec="milliseconds"),
                "payload": row.asDict(),
            }, ensure_ascii=False, default=str)

        symbol_key = getattr(row, "symbol", "UNKNOWN")
        yield symbol_key, envelope_json


def _publish_to_kafka_batch(
    spark: SparkSession,
    messages: list[tuple[str, str]],
    kafka_topic: str,
) -> int:
    """Publica un batch de (key, value) strings a Kafka usando Spark."""
    if not messages:
        return 0

    rows_rdd = spark.sparkContext.parallelize(messages)
    messages_df = spark.createDataFrame(rows_rdd, ["key", "value"])

    (
        messages_df
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS)
        .option("topic", kafka_topic)
        .save()
    )
    return len(messages)


# ── Entrypoint principal ───────────────────────────────────────────────────────

def run_replay(
    spark: SparkSession,
    source: str,
    replay_speed_factor: float,
    symbol_filter: str | None,
    date_filter: str | None,
    dry_run: bool,
    kafka_publish_batch_size: int = 500,
) -> None:
    kafka_topic = SOURCE_KAFKA_TOPICS[source]

    df = _load_parquet(spark, source, symbol_filter, date_filter)
    total_row_count = df.count()
    print(f"Total de filas a reproducir: {total_row_count:,}")

    if dry_run:
        print("[dry-run] Sin publicar a Kafka. Fin.")
        return

    print(f"Publicando a topic: {kafka_topic}")
    speed_label = f"{replay_speed_factor}x" if replay_speed_factor > 0 else "máxima velocidad"
    print(f"Velocidad de replay: {speed_label}")
    print("-" * 50)

    replay_start_wall_clock = time.monotonic()
    published_message_count = 0
    pending_batch: list[tuple[str, str]] = []

    for symbol_key, envelope_json in _iter_rows_with_throttle(df, source, replay_speed_factor):
        pending_batch.append((symbol_key, envelope_json))

        if len(pending_batch) >= kafka_publish_batch_size:
            published_message_count += _publish_to_kafka_batch(spark, pending_batch, kafka_topic)
            pending_batch = []
            elapsed_seconds = time.monotonic() - replay_start_wall_clock
            messages_per_second = published_message_count / elapsed_seconds if elapsed_seconds > 0 else 0
            print(
                f"  Publicados: {published_message_count:,} / {total_row_count:,}"
                f"  ({messages_per_second:.0f} msg/s)"
            )

    # Flush del batch final
    if pending_batch:
        published_message_count += _publish_to_kafka_batch(spark, pending_batch, kafka_topic)

    total_elapsed_seconds = time.monotonic() - replay_start_wall_clock
    avg_messages_per_second = published_message_count / total_elapsed_seconds if total_elapsed_seconds > 0 else 0

    print("-" * 50)
    print(f"Replay completado:")
    print(f"  Mensajes publicados : {published_message_count:,}")
    print(f"  Tiempo total        : {total_elapsed_seconds:.1f} s")
    print(f"  Throughput promedio : {avg_messages_per_second:.0f} msg/s")


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_arguments() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Replay de Parquet histórico → Kafka para benchmarks CPU vs GPU"
    )
    parser.add_argument(
        "--source",
        choices=["features", "trades", "predictions"],
        default="features",
        help="Directorio Parquet a reproducir (default: features)",
    )
    parser.add_argument(
        "--speed",
        type=float,
        default=1.0,
        dest="replay_speed_factor",
        help="Factor de velocidad: 1.0=tiempo real, 2.0=doble, 0.0=sin throttle (default: 1.0)",
    )
    parser.add_argument(
        "--symbol",
        type=str,
        default=None,
        dest="symbol_filter",
        help="Filtrar por símbolo (ej: BTCUSDT). Default: todos.",
    )
    parser.add_argument(
        "--date",
        type=str,
        default=None,
        dest="date_filter",
        help="Filtrar por fecha ISO (ej: 2024-01-15). Default: todos.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        default=False,
        help="Parsear y contar filas sin publicar a Kafka.",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=500,
        dest="kafka_publish_batch_size",
        help="Número de mensajes por batch de escritura a Kafka (default: 500)",
    )
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_arguments()

    spark = (
        SparkSession.builder
        .appName("crypto-parquet-replayer")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    run_replay(
        spark=spark,
        source=args.source,
        replay_speed_factor=args.replay_speed_factor,
        symbol_filter=args.symbol_filter,
        date_filter=args.date_filter,
        dry_run=args.dry_run,
        kafka_publish_batch_size=args.kafka_publish_batch_size,
    )
