#!/usr/bin/env bash
# Crea los 4 topics del proyecto con 8 particiones y replication-factor 1.
# Requiere que el contenedor kafka-kraft esté corriendo.
# Uso: bash infra/scripts/create_topics.sh

set -euo pipefail

KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
KAFKA_CONTAINER_NAME="kafka-kraft"
TOPIC_PARTITIONS=8
TOPIC_REPLICATION_FACTOR=1

TOPICS=(
  "crypto-trades"
  "crypto-book"
  "crypto-features"
  "crypto-pred"
)

echo "Esperando a que Kafka esté listo..."
until docker exec "${KAFKA_CONTAINER_NAME}" kafka-topics \
  --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" \
  --list > /dev/null 2>&1; do
  sleep 2
done
echo "Kafka listo."

for TOPIC_NAME in "${TOPICS[@]}"; do
  docker exec "${KAFKA_CONTAINER_NAME}" kafka-topics \
    --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" \
    --create \
    --if-not-exists \
    --topic "${TOPIC_NAME}" \
    --partitions "${TOPIC_PARTITIONS}" \
    --replication-factor "${TOPIC_REPLICATION_FACTOR}"
  echo "  ✓ ${TOPIC_NAME}"
done

echo ""
echo "Topics creados. Estado actual:"
docker exec "${KAFKA_CONTAINER_NAME}" kafka-topics \
  --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" \
  --describe
