"""
gpu_smoke_test.py — Benchmark de agregaciones SQL sobre Parquet pre-generado.

Lee `~/data/crypto/synth.parquet` (generado una vez con gen_synth_parquet.py)
y ejecuta groupBy.agg + correlación BTC. Se corre dos veces:
  - CPU: bash infra/scripts/run_spark_cpu.sh spark/jobs/gpu_smoke_test.py
  - GPU: bash infra/scripts/run_spark_gpu.sh spark/jobs/gpu_smoke_test.py

La diferencia la hacen los flags de spark-submit. El script es idéntico.
Lectura Parquet → GPU es el camino donde RAPIDS realmente acelera
(columnar nativo, sin conversiones row↔columnar).

Path del Parquet desde env SYNTH_PARQUET; default ~/data/crypto/synth.parquet.
"""
from __future__ import annotations

import os
import time
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

try:
    from spark.jobs.batch_aggregate import _compute_symbol_stats
except ImportError:
    def _compute_symbol_stats(df: DataFrame) -> DataFrame:
        return df.groupBy("symbol").agg(
            F.count("*").alias("total_windows"),
            F.avg("vwap").alias("mean_vwap"),
            F.stddev("vwap").alias("stddev_vwap"),
            F.min("avg_price").alias("historical_min_price"),
            F.max("avg_price").alias("historical_max_price"),
            F.avg("price_volatility").alias("mean_volatility"),
            F.max("price_volatility").alias("peak_volatility"),
            F.avg("total_volume").alias("avg_volume_per_window"),
            F.sum("total_volume").alias("total_historical_volume"),
            F.avg("avg_spread_proxy").alias("mean_spread"),
            F.avg("trade_count").alias("avg_trades_per_window"),
            F.percentile_approx("vwap", 0.50).alias("vwap_p50"),
            F.percentile_approx("price_volatility", 0.95).alias("volatility_p95"),
        )


DEFAULT_PARQUET = str(Path.home() / "data" / "crypto" / "synth.parquet")


def _btc_correlation_broadcast(df: DataFrame) -> DataFrame:
    btc = df.filter(F.col("symbol") == "BTCUSDT").select(
        F.col("window_start"), F.col("vwap").alias("btc_vwap")
    )
    return (
        df.join(F.broadcast(btc), on="window_start", how="inner")
        .groupBy("symbol")
        .agg(F.corr("vwap", "btc_vwap").alias("corr_with_btc"))
        .orderBy(F.col("corr_with_btc").desc())
    )


def run(spark: SparkSession, parquet_path: str) -> None:
    print(f"Leyendo Parquet: {parquet_path}")
    df = spark.read.parquet(parquet_path)

    n = df.count()
    print(f"Filas: {n:,}")

    t0 = time.time()

    print("Calculando estadísticas por símbolo...")
    stats = _compute_symbol_stats(df)

    print("Calculando correlaciones con BTCUSDT...")
    corr = _btc_correlation_broadcast(df)

    print("\n=== Estadísticas por símbolo ===")
    stats.select("symbol", "total_windows", "mean_vwap", "mean_volatility").show(truncate=False)

    print("\n=== Correlación con BTCUSDT ===")
    corr.show(truncate=False)

    elapsed = time.time() - t0
    print(f"\n{'='*50}")
    print(f"TIEMPO TOTAL (agregaciones): {elapsed:.1f}s")
    print(f"{'='*50}")
    print("Capturas Spark UI: http://localhost:4040")


if __name__ == "__main__":
    parquet_path = os.environ.get("SYNTH_PARQUET", DEFAULT_PARQUET)
    spark = (
        SparkSession.builder
        .appName("crypto-gpu-smoke-test")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    run(spark, parquet_path)
