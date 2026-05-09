#!/usr/bin/env bash
# Ejecuta un job de Spark en modo CPU (sin RAPIDS).
# Uso: bash infra/scripts/run_spark_cpu.sh spark/jobs/streaming_features.py
#
# Variables de entorno opcionales (sobreescriben defaults):
#   SPARK_DRIVER_MEMORY   (default: 8g)
#   SPARK_EXECUTOR_MEMORY (default: 16g)
#   SPARK_EXECUTOR_CORES  (default: 10)
#   SPARK_SQL_SHUFFLE_PARTITIONS (default: 20)

set -euo pipefail

SPARK_JOB_SCRIPT="${1:?Uso: $0 <ruta/al/job.py>}"

SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-8g}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-16g}"
SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-10}"
SPARK_SQL_SHUFFLE_PARTITIONS="${SPARK_SQL_SHUFFLE_PARTITIONS:-20}"

export PYSPARK_PYTHON=python3.11

echo "Iniciando Spark (CPU) — job: ${SPARK_JOB_SCRIPT}"
echo "  driver.memory=${SPARK_DRIVER_MEMORY}  executor.memory=${SPARK_EXECUTOR_MEMORY}  executor.cores=${SPARK_EXECUTOR_CORES}"

spark-submit \
  --master "local[${SPARK_EXECUTOR_CORES}]" \
  --driver-memory "${SPARK_DRIVER_MEMORY}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY}" \
  --conf "spark.executor.cores=${SPARK_EXECUTOR_CORES}" \
  --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS}" \
  --conf "spark.ui.port=4040" \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4" \
  "${SPARK_JOB_SCRIPT}"
