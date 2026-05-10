"""
inflate_features.py — Multiplica el dataset de features para benchmarks GPU.

Lee ${DATA_DIR}/features (cientos de archivos pequeños, ~1.6k filas reales),
genera N copias con timestamps desplazados y perturbaciones determinísticas en precios,
y escribe el resultado a ${DATA_DIR}/features_inflated en pocos archivos grandes
listos para consumir por batch_aggregate.

Usa crossJoin con spark.range(copies) en lugar de reduce+unionByName para mantener
un plan de ejecución compacto (2 scans) independientemente de N.

Uso:
    python3.11 spark/replayer/inflate_features.py --copies 6000
    python3.11 spark/replayer/inflate_features.py --copies 100 --output-dir /tmp/features_small
    python3.11 spark/replayer/inflate_features.py --copies 6000 --partitions 40

Después apuntar batch_aggregate al nuevo directorio:
    FEATURES_PARQUET_DIR=$DATA_DIR/features_inflated COMPACT_FEATURES=false \
        DATA_DIR=... bash infra/scripts/run_spark_gpu.sh spark/jobs/batch_aggregate.py
"""
from __future__ import annotations

import argparse
import os
import time

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

load_dotenv()

DATA_DIR: str = os.environ["DATA_DIR"]
FEATURES_PARQUET_DIR: str = f"{DATA_DIR}/features"
DEFAULT_OUTPUT_DIR: str = f"{DATA_DIR}/features_inflated"

WINDOW_GAP_SECONDS: int = 5
PRICE_NOISE_BPS: float = 10.0


def _inflate(spark: SparkSession, df: DataFrame, copies: int) -> DataFrame:
    """Genera copies × len(df) filas usando crossJoin con un DataFrame de índices.

    Plan resultante: 2 scans + 1 cross join — tamaño fijo sin importar copies.
    El ruido es determinístico (sin rand()) para que CPU y GPU produzcan el mismo
    dataset y la comparativa sea válida.
    """
    idx = spark.range(copies).toDF("copy_idx")
    crossed = df.crossJoin(idx)

    shift_seconds = F.col("copy_idx") * F.lit(WINDOW_GAP_SECONDS)
    # sin(copy_idx) oscila entre -1 y 1 → ruido ±PRICE_NOISE_BPS bps
    noise = F.lit(1.0) + F.sin(F.col("copy_idx").cast("double")) * F.lit(PRICE_NOISE_BPS / 10000.0)

    out = (
        crossed
        .withColumn(
            "window_start",
            (F.col("window_start").cast("long") + shift_seconds).cast("timestamp"),
        )
        .withColumn("vwap", F.col("vwap") * noise)
        .withColumn("avg_price", F.col("avg_price") * noise)
        .withColumn("avg_best_bid_price", F.col("avg_best_bid_price") * noise)
        .withColumn("avg_best_ask_price", F.col("avg_best_ask_price") * noise)
    )
    if "window_end" in df.columns:
        out = out.withColumn(
            "window_end",
            (F.col("window_end").cast("long") + shift_seconds).cast("timestamp"),
        )
    if "date" in df.columns:
        out = out.withColumn("date", F.to_date("window_start"))

    return out.drop("copy_idx")


def run_inflate(spark: SparkSession, copies: int, output_dir: str, partitions: int) -> None:
    print(f"Leyendo base desde: {FEATURES_PARQUET_DIR}")
    base = spark.read.parquet(FEATURES_PARQUET_DIR).cache()
    base_count = base.count()
    print(f"Filas base: {base_count:,}")
    print(f"Generando {copies} copias → ~{base_count * copies:,} filas estimadas")

    t0 = time.time()
    inflated = _inflate(spark, base, copies).repartition(partitions, "symbol")

    print(f"Escribiendo a: {output_dir} ({partitions} particiones)")
    inflated.write.mode("overwrite").parquet(output_dir)

    elapsed = time.time() - t0
    final_count = spark.read.parquet(output_dir).count()
    print(f"Listo en {elapsed:.1f}s — filas finales: {final_count:,}")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Inflar features Parquet para benchmark GPU")
    parser.add_argument("--copies", type=int, default=6000,
                        help="Número de copias del dataset base (default: 6000)")
    parser.add_argument("--output-dir", type=str, default=DEFAULT_OUTPUT_DIR,
                        help=f"Directorio de salida (default: {DEFAULT_OUTPUT_DIR})")
    parser.add_argument("--partitions", type=int, default=40,
                        help="Particiones de salida (default: 40)")
    return parser.parse_args()


if __name__ == "__main__":
    args = _parse_args()
    spark = (
        SparkSession.builder
        .appName("crypto-inflate-features")
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MICROS")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    run_inflate(spark, args.copies, args.output_dir, args.partitions)
