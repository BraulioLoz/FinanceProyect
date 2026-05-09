# infra/ — Infraestructura Docker + WSL

Única pieza de infraestructura gestionada: **Apache Kafka en modo KRaft** (sin Zookeeper), ejecutado en Docker dentro de WSL2.

---

## Docker Compose — Kafka KRaft

Archivo: `infra/docker-compose.kafka.yml`

```yaml
version: "3.8"
services:
  kafka:
    image: confluentinc/cp-kafka:7.6.1
    container_name: kafka-kraft
    ports:
      - "9092:9092"
    environment:
      KAFKA_NODE_ID: 1
      KAFKA_PROCESS_ROLES: broker,controller
      KAFKA_LISTENERS: PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093
      KAFKA_ADVERTISED_LISTENERS: PLAINTEXT://localhost:9092
      KAFKA_CONTROLLER_QUORUM_VOTERS: 1@localhost:9093
      KAFKA_CONTROLLER_LISTENER_NAMES: CONTROLLER
      KAFKA_INTER_BROKER_LISTENER_NAME: PLAINTEXT
      KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR: 1
      KAFKA_AUTO_CREATE_TOPICS_ENABLE: "false"
      CLUSTER_ID: "MkU3OEVBNTcwNTJENDM2Qg"
    volumes:
      - kafka-data:/var/lib/kafka/data

volumes:
  kafka-data:
```

**Levantar Kafka:**
```bash
cd infra
docker compose -f docker-compose.kafka.yml up -d
```

**Verificar que está corriendo:**
```bash
docker compose -f docker-compose.kafka.yml ps
docker logs kafka-kraft --tail 20
```

**Detener:**
```bash
docker compose -f docker-compose.kafka.yml down
```

---

## Comandos WSL — gestión de topics

Script: `infra/scripts/create_topics.sh`

```bash
#!/usr/bin/env bash
# Crear los 4 topics con 8 particiones y replication-factor 1
BOOTSTRAP="localhost:9092"

for TOPIC in crypto-trades crypto-book crypto-features crypto-pred; do
  docker exec kafka-kraft kafka-topics \
    --bootstrap-server "$BOOTSTRAP" \
    --create \
    --if-not-exists \
    --topic "$TOPIC" \
    --partitions 8 \
    --replication-factor 1
  echo "Topic creado: $TOPIC"
done
```

**Listar topics:**
```bash
docker exec kafka-kraft kafka-topics --bootstrap-server localhost:9092 --list
```

**Ver detalles de un topic:**
```bash
docker exec kafka-kraft kafka-topics --bootstrap-server localhost:9092 --describe --topic crypto-trades
```

**Eliminar un topic (si hace falta resetear):**
```bash
docker exec kafka-kraft kafka-topics --bootstrap-server localhost:9092 --delete --topic crypto-trades
```

---

## Ejecutar Spark — CPU

Script: `infra/scripts/run_spark_cpu.sh`

Host: i7-12700H / 32 GB RAM — configuración en `spark/conf/spark-cpu.conf`.

```bash
#!/usr/bin/env bash
export PYSPARK_PYTHON=python3.11
spark-submit \
  --driver-memory 8g \
  --executor-memory 16g \
  --executor-cores 10 \
  --conf spark.sql.shuffle.partitions=20 \
  --properties-file spark/conf/spark-cpu.conf \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 \
  "$1"
```

Uso: `./infra/scripts/run_spark_cpu.sh spark/jobs/streaming_features.py`

---

## Ejecutar Spark — GPU (RAPIDS)

Script: `infra/scripts/run_spark_gpu.sh`

Host: RTX 4070 Laptop 8 GB VRAM — configuración en `spark/conf/spark-gpu.conf`.
Confirmar versión de `rapids-4-spark` compatible con Spark 3.5.x + CUDA 12.x antes de correr.

```bash
#!/usr/bin/env bash
# Rutas a los JARs de RAPIDS — ajustar según versión instalada en WSL
RAPIDS_JAR_PATH="${RAPIDS_JAR_PATH:-$HOME/jars/rapids-4-spark_2.12-<ver>.jar}"
CUDF_JAR_PATH="${CUDF_JAR_PATH:-$HOME/jars/cudf-<ver>-cuda12.jar}"

export PYSPARK_PYTHON=python3.11
spark-submit \
  --driver-memory 8g \
  --executor-memory 16g \
  --executor-cores 10 \
  --conf spark.sql.shuffle.partitions=20 \
  --conf spark.rapids.memory.pinnedPool.size=2g \
  --conf spark.executor.resource.gpu.amount=1 \
  --conf spark.task.resource.gpu.amount=1 \
  --properties-file spark/conf/spark-gpu.conf \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.4 \
  --jars "${RAPIDS_JAR_PATH},${CUDF_JAR_PATH}" \
  "$1"
```

Uso: `./infra/scripts/run_spark_gpu.sh spark/jobs/streaming_features.py`

---

## NVIDIA / CUDA en WSL2

**Regla importante:** el driver NVIDIA se instala **solo en Windows**. Dentro de WSL2:

- Instalar únicamente el **CUDA Toolkit 12.x** (sin `nvidia-driver-*`).
- Instalar las librerías RAPIDS según la [documentación oficial de NVIDIA](https://docs.nvidia.com/cuda/wsl-user-guide/).
- **No ejecutar** `sudo apt install nvidia-driver-*` dentro de WSL — rompe el passthrough GPU de Windows.

Verificar que la GPU es visible en WSL:
```bash
nvidia-smi   # debe mostrar la GPU y CUDA 12.x
```

Script opcional de instalación de CUDA en WSL: `infra/scripts/install_cuda12_wsl.sh` (si el repo lo incluye).

---

## Estructura de archivos

```
infra/
  README.md                    # este archivo
  docker-compose.kafka.yml     # Kafka KRaft
  scripts/
    create_topics.sh           # crear los 4 topics
    run_spark_cpu.sh           # lanzar Spark sin RAPIDS
    run_spark_gpu.sh           # lanzar Spark con RAPIDS
    install_cuda12_wsl.sh      # (opcional) setup CUDA en WSL
```
