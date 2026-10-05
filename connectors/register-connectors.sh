#!/bin/bash
echo "Starting connector registration process..."

CONNECT_URL="${CONNECT_URL:-http://localhost:8083}"
RETRIES=0
MAX_RETRIES=40
SLEEP_INTERVAL=5

echo "Waiting for Kafka Connect REST API to become available..."

until curl -s ${CONNECT_URL}/connectors >/dev/null 2>&1; do
  RETRIES=$((RETRIES+1))
  echo "  → Still waiting... (try ${RETRIES}/${MAX_RETRIES})"
  sleep ${SLEEP_INTERVAL}
  if [ ${RETRIES} -ge ${MAX_RETRIES} ]; then
    echo "Timeout: Kafka Connect REST API not ready after $((RETRIES * SLEEP_INTERVAL))s"
    exit 1
  fi
done

echo "Kafka Connect REST API is ready!"

echo "Registering connectors from JSON files..."

CONNECTORS_DIR="$(dirname "$0")"

FOUND=false

for f in ${CONNECTORS_DIR}/*.json; do
  if [ -f "$f" ]; then
    FOUND=true
    echo "→ Registering connector: $f"
    curl -s -X POST -H "Content-Type: application/json" --data @"$f" ${CONNECT_URL}/connectors \
      && echo "Registered: $f" \
      || echo "Failed: $f"
  fi
done

if [ "$FOUND" = false ]; then
  echo "No connector JSON files found in ${CONNECTORS_DIR}"
fi

echo "All connectors processed!"