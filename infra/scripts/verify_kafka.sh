#!/usr/bin/env bash
# Verifica que Kafka está corriendo y los 4 topics existen con la config esperada.
# Uso: bash infra/scripts/verify_kafka.sh

set -euo pipefail

KAFKA_BOOTSTRAP_SERVERS="localhost:9092"
KAFKA_CONTAINER_NAME="kafka-kraft"
EXPECTED_TOPICS=("crypto-trades" "crypto-book" "crypto-features" "crypto-pred")
EXPECTED_PARTITION_COUNT=8

echo "=== Estado del contenedor ==="
docker ps --filter "name=${KAFKA_CONTAINER_NAME}" --format "table {{.Names}}\t{{.Status}}\t{{.Ports}}"

echo ""
echo "=== Topics existentes ==="
docker exec "${KAFKA_CONTAINER_NAME}" kafka-topics \
  --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" \
  --list

echo ""
echo "=== Verificando topics esperados ==="
ALL_OK=true
for TOPIC_NAME in "${EXPECTED_TOPICS[@]}"; do
  PARTITION_COUNT=$(docker exec "${KAFKA_CONTAINER_NAME}" kafka-topics \
    --bootstrap-server "${KAFKA_BOOTSTRAP_SERVERS}" \
    --describe \
    --topic "${TOPIC_NAME}" 2>/dev/null \
    | grep -c "Partition:" || true)

  if [[ "${PARTITION_COUNT}" -eq "${EXPECTED_PARTITION_COUNT}" ]]; then
    echo "  ✓ ${TOPIC_NAME} (${PARTITION_COUNT} particiones)"
  else
    echo "  ✗ ${TOPIC_NAME} — esperadas ${EXPECTED_PARTITION_COUNT}, encontradas ${PARTITION_COUNT}"
    ALL_OK=false
  fi
done

echo ""
if [[ "${ALL_OK}" == "true" ]]; then
  echo "Todo OK — Kafka listo para recibir mensajes."
else
  echo "Hay topics faltantes. Correr: bash infra/scripts/create_topics.sh"
  exit 1
fi
