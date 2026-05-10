"""
gen_synth_parquet.py — Genera Parquet sintético para benchmark CPU vs GPU.

Corre UNA SOLA VEZ en modo CPU. Escribe a ~/data/crypto/synth.parquet/
particionado por `symbol`. Después `gpu_smoke_test.py` lee este Parquet
en CPU y GPU para medir solo agregaciones SQL (apples-to-apples).

Uso:
    bash infra/scripts/run_spark_cpu.sh spark/jobs/gen_synth_parquet.py
    bash infra/scripts/run_spark_cpu.sh spark/jobs/gen_synth_parquet.py --rows-per-symbol 500000
"""
from __future__ import annotations

import argparse
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

DEFAULT_ROWS_PER_SYMBOL = 97_000
DEFAULT_OUT = str(Path.home() / "data" / "crypto" / "synth.parquet")


def _build(spark: SparkSession, rows_per_symbol: int) -> DataFrame:
    total_rows = rows_per_symbol * 10
    idx = (F.col("id") % F.lit(10)).cast("int")
    id_d = F.col("id").cast("double")

    symbol_expr = (
        F.when(idx == 0, "BTCUSDT").when(idx == 1, "ETHUSDT")
        .when(idx == 2, "BNBUSDT").when(idx == 3, "SOLUSDT")
        .when(idx == 4, "XRPUSDT").when(idx == 5, "ADAUSDT")
        .when(idx == 6, "DOGEUSDT").when(idx == 7, "AVAXUSDT")
        .when(idx == 8, "DOTUSDT").otherwise("MATICUSDT")
    )
    base_price_expr = (
        F.when(idx == 0, 65000.0).when(idx == 1, 3500.0)
        .when(idx == 2, 580.0).when(idx == 3, 170.0)
        .when(idx == 4, 0.65).when(idx == 5, 0.45)
        .when(idx == 6, 0.18).when(idx == 7, 38.0)
        .when(idx == 8, 8.5).otherwise(0.85)
    )
    noise = F.lit(1.0) + F.sin(id_d * F.lit(0.001)) * F.lit(0.02)

    return (
        spark.range(total_rows)
        .withColumn("symbol", symbol_expr)
        .withColumn("_base", base_price_expr)
        .withColumn("vwap", F.col("_base") * noise)
        .withColumn("avg_price", F.col("_base") * (noise + F.lit(0.001)))
        .withColumn("price_volatility", F.abs(F.sin(id_d)) * F.lit(0.05))
        .withColumn("total_volume", F.lit(1000.0) + F.cos(id_d) * F.lit(500.0))
        .withColumn("trade_count", (F.col("id") % F.lit(200) + F.lit(10)).cast("long"))
        .withColumn("avg_spread_proxy", F.col("_base") * F.lit(0.0005))
        .withColumn("avg_best_bid_price", F.col("vwap") * F.lit(0.9995))
        .withColumn("avg_best_ask_price", F.col("vwap") * F.lit(1.0005))
        .withColumn("min_price", F.col("vwap") * F.lit(0.995))
        .withColumn("max_price", F.col("vwap") * F.lit(1.005))
        .withColumn("window_start", (F.lit(1_715_000_000) + F.col("id") * F.lit(5)).cast("timestamp"))
        .drop("id", "_base")
    )


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows-per-symbol", type=int, default=DEFAULT_ROWS_PER_SYMBOL)
    parser.add_argument("--out", type=str, default=os.environ.get("SYNTH_PARQUET", DEFAULT_OUT))
    args = parser.parse_args()

    total = args.rows_per_symbol * 10
    print(f"Generando {total:,} filas (10 símbolos × {args.rows_per_symbol:,}) → {args.out}")

    spark = SparkSession.builder.appName("gen-synth-parquet").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    df = _build(spark, args.rows_per_symbol)
    (
        df.write
        .mode("overwrite")
        .partitionBy("symbol")
        .parquet(args.out)
    )

    print(f"OK — escrito a {args.out}")
    spark.stop()


if __name__ == "__main__":
    main()
