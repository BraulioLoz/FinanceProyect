"""
batch_aggregate.py — Benchmark GPU-friendly: agregaciones masivas sobre Parquet histórico.

Este job está diseñado específicamente para mostrar la ventaja de RAPIDS GPU sobre CPU.
A diferencia del streaming stateful (no acelerado por RAPIDS), las operaciones de este
job (groupBy, agg, sort, join sobre DataFrames estáticos) SÍ corren en GPU.

Uso:
    CPU: bash infra/scripts/run_spark_cpu.sh spark/jobs/batch_aggregate.py
    GPU: bash infra/scripts/run_spark_gpu.sh spark/jobs/batch_aggregate.py
"""
from __future__ import annotations

import os
import time

from dotenv import load_dotenv
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql import Window

load_dotenv()

DATA_DIR: str = os.environ["DATA_DIR"]
FEATURES_PARQUET_DIR: str = f"{DATA_DIR}/features"
AGGREGATES_PARQUET_DIR: str = f"{DATA_DIR}/aggregates"


def _compute_symbol_stats(df: DataFrame) -> DataFrame:
    """Estadísticas globales por símbolo sobre todo el histórico."""
    return df.groupBy("symbol").agg(
        F.count("*").alias("total_windows"),
        F.avg("vwap").alias("mean_vwap"),
        F.stddev("vwap").alias("stddev_vwap"),
        F.min("min_price").alias("historical_min_price"),
        F.max("max_price").alias("historical_max_price"),
        F.avg("price_volatility").alias("mean_volatility"),
        F.max("price_volatility").alias("peak_volatility"),
        F.avg("total_volume").alias("avg_volume_per_window"),
        F.sum("total_volume").alias("total_historical_volume"),
        F.avg("avg_spread_proxy").alias("mean_spread"),
        F.min("avg_spread_proxy").alias("min_spread"),
        F.max("avg_spread_proxy").alias("max_spread"),
        F.avg("trade_count").alias("avg_trades_per_window"),
        F.percentile_approx("vwap", 0.25).alias("vwap_p25"),
        F.percentile_approx("vwap", 0.50).alias("vwap_p50"),
        F.percentile_approx("vwap", 0.75).alias("vwap_p75"),
        F.percentile_approx("price_volatility", 0.95).alias("volatility_p95"),
    )


def _compute_rolling_vwap(df: DataFrame) -> DataFrame:
    """VWAP rolling de 5 ventanas por símbolo."""
    window_spec = (
        Window.partitionBy("symbol")
        .orderBy("window_start")
        .rowsBetween(-4, 0)
    )
    return df.withColumn("rolling_vwap_5w", F.avg("vwap").over(window_spec))


def _compute_cross_symbol_correlation(df: DataFrame) -> DataFrame:
    """Correlación de VWAP entre pares de símbolos por ventana temporal."""
    btc = df.filter(F.col("symbol") == "BTCUSDT").select(
        F.col("window_start"),
        F.col("vwap").alias("btc_vwap"),
    )
    return (
        df.join(btc, on="window_start", how="inner")
        .groupBy("symbol")
        .agg(F.corr("vwap", "btc_vwap").alias("corr_with_btc"))
        .orderBy(F.col("corr_with_btc").desc())
    )


def run_batch_aggregate(spark: SparkSession) -> None:
    print(f"Leyendo Parquet desde: {FEATURES_PARQUET_DIR}")
    t0 = time.time()

    df = spark.read.parquet(FEATURES_PARQUET_DIR)
    total_rows = df.count()
    print(f"Filas totales: {total_rows:,}")

    # 1. Estadísticas por símbolo
    print("Calculando estadísticas por símbolo...")
    symbol_stats = _compute_symbol_stats(df)
    symbol_stats.cache()
    symbol_stats.count()

    # 2. VWAP rolling
    print("Calculando VWAP rolling...")
    rolling_df = _compute_rolling_vwap(df)

    # 3. Correlación con BTC
    print("Calculando correlaciones con BTCUSDT...")
    corr_df = _compute_cross_symbol_correlation(df)

    # Mostrar resultados
    print("\n=== Estadísticas por símbolo ===")
    symbol_stats.select(
        "symbol", "total_windows", "mean_vwap", "stddev_vwap",
        "historical_min_price", "historical_max_price", "mean_volatility",
    ).show(truncate=False)

    print("\n=== Correlación con BTCUSDT ===")
    corr_df.show(truncate=False)

    # Guardar resultados
    print(f"\nGuardando agregados en: {AGGREGATES_PARQUET_DIR}")
    symbol_stats.write.mode("overwrite").parquet(f"{AGGREGATES_PARQUET_DIR}/symbol_stats")
    corr_df.write.mode("overwrite").parquet(f"{AGGREGATES_PARQUET_DIR}/btc_correlations")

    elapsed = time.time() - t0
    print(f"\n{'='*50}")
    print(f"TIEMPO TOTAL: {elapsed:.1f}s ({elapsed/60:.1f} min)")
    print(f"{'='*50}")
    print("Toma capturas de Spark UI ahora: http://localhost:4040")


if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName("crypto-batch-aggregate")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    run_batch_aggregate(spark)
