#!/usr/bin/env bash
# Ejecuta un job de Spark en modo GPU con RAPIDS Accelerator.
# Requiere CUDA 12.x instalado en WSL y los JARs de rapids-4-spark.
# Uso: bash infra/scripts/run_spark_gpu.sh spark/jobs/streaming_features.py
#
# Variables de entorno requeridas (o configurar en .env):
#   RAPIDS_JAR_PATH   ruta al jar rapids-4-spark_2.12-<ver>.jar
#   CUDF_JAR_PATH     ruta al jar cudf-<ver>-cuda12.jar
#
# Variables de entorno opcionales:
#   SPARK_DRIVER_MEMORY            (default: 8g)
#   SPARK_EXECUTOR_MEMORY          (default: 16g)
#   SPARK_EXECUTOR_CORES           (default: 10)
#   SPARK_SQL_SHUFFLE_PARTITIONS   (default: 20)
#   SPARK_RAPIDS_PINNED_POOL_SIZE  (default: 2g)

set -euo pipefail

SPARK_JOB_SCRIPT="${1:?Uso: $0 <ruta/al/job.py>}"

# Cargar .env si existe
if [[ -f ".env" ]]; then
  set -o allexport
  source .env
  set +o allexport
fi

RAPIDS_JAR_PATH="${RAPIDS_JAR_PATH:?Definir RAPIDS_JAR_PATH en .env o como variable de entorno}"
CUDF_JAR_PATH="${CUDF_JAR_PATH:?Definir CUDF_JAR_PATH en .env o como variable de entorno}"

SPARK_DRIVER_MEMORY="${SPARK_DRIVER_MEMORY:-8g}"
SPARK_EXECUTOR_MEMORY="${SPARK_EXECUTOR_MEMORY:-16g}"
SPARK_EXECUTOR_CORES="${SPARK_EXECUTOR_CORES:-10}"
SPARK_SQL_SHUFFLE_PARTITIONS="${SPARK_SQL_SHUFFLE_PARTITIONS:-20}"
SPARK_RAPIDS_PINNED_POOL_SIZE="${SPARK_RAPIDS_PINNED_POOL_SIZE:-2g}"

export PYSPARK_PYTHON=python3.11

echo "Iniciando Spark (GPU/RAPIDS) — job: ${SPARK_JOB_SCRIPT}"
echo "  driver.memory=${SPARK_DRIVER_MEMORY}  executor.memory=${SPARK_EXECUTOR_MEMORY}  executor.cores=${SPARK_EXECUTOR_CORES}"
echo "  RAPIDS JAR: ${RAPIDS_JAR_PATH}"
echo "  CUDF  JAR: ${CUDF_JAR_PATH}"

spark-submit \
  --master "local[${SPARK_EXECUTOR_CORES}]" \
  --driver-memory "${SPARK_DRIVER_MEMORY}" \
  --executor-memory "${SPARK_EXECUTOR_MEMORY}" \
  --conf "spark.executor.cores=${SPARK_EXECUTOR_CORES}" \
  --conf "spark.sql.shuffle.partitions=${SPARK_SQL_SHUFFLE_PARTITIONS}" \
  --conf "spark.ui.port=4040" \
  --conf "spark.plugins=com.nvidia.spark.SQLPlugin" \
  --conf "spark.rapids.memory.pinnedPool.size=${SPARK_RAPIDS_PINNED_POOL_SIZE}" \
  --conf "spark.executor.resource.gpu.amount=1" \
  --conf "spark.task.resource.gpu.amount=1" \
  --conf "spark.rapids.sql.concurrentGpuTasks=2" \
  --conf "spark.sql.session.timeZone=UTC" \
  --conf "spark.driver.extraJavaOptions=-Duser.timezone=UTC" \
  --conf "spark.executor.extraJavaOptions=-Duser.timezone=UTC" \
  --conf "spark.rapids.sql.castStringToTimestamp.enabled=true" \
  --conf "spark.rapids.sql.hasExtendedYearValues=false" \
  --packages "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4" \
  --jars "${RAPIDS_JAR_PATH},${CUDF_JAR_PATH}" \
  "${SPARK_JOB_SCRIPT}"
