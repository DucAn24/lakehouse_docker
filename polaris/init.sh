#!/bin/sh
# Idempotent Polaris setup for the lakehouse: the `lakehouse` Iceberg catalog on MinIO, the grant
# that lets the root principal manage its content, and the bronze/silver/gold/monitoring namespaces.
# Run by the compose `polaris-init` service and by the test harness (tests/iceberg_lake.py).
#
# Storage: only `endpointInternal` is set (Polaris itself writes metadata there). Without a
# client-facing `endpoint`, Polaris sends no `s3.endpoint` to clients, so every engine keeps its own
# MinIO address (minio:9000 inside Docker, localhost:9000 on the host). stsUnavailable: no
# credential vending, clients use their own MinIO keys.
set -eu

POLARIS=${POLARIS_HOST_URL:-http://polaris:8181}
CATALOG=${ICEBERG_CATALOG:-lakehouse}
BUCKET=${LAKEHOUSE_BUCKET:-lakehouse}
S3_INTERNAL=${S3_ENDPOINT:-http://minio:9000}
REGION=${AWS_REGION:-us-east-1}
NAMESPACES="bronze silver gold monitoring"

TOKEN=$(curl -sS --fail-with-body "$POLARIS/api/catalog/v1/oauth/tokens" \
  --user "$POLARIS_CLIENT_ID:$POLARIS_CLIENT_SECRET" \
  -d grant_type=client_credentials -d scope=PRINCIPAL_ROLE:ALL \
  | sed -n 's/.*"access_token" *: *"\([^"]*\)".*/\1/p')
if [ -z "$TOKEN" ]; then
  echo "✗ Could not get a Polaris token for client $POLARIS_CLIENT_ID"
  exit 1
fi
AUTH="Authorization: Bearer $TOKEN"
JSON="Content-Type: application/json"
MGMT="$POLARIS/api/management/v1"

if curl -sf -o /dev/null -H "$AUTH" "$MGMT/catalogs/$CATALOG"; then
  echo "✓ Catalog '$CATALOG' already exists"
else
  curl -sS --fail-with-body -o /dev/null -X POST -H "$AUTH" -H "$JSON" "$MGMT/catalogs" -d "{
    \"catalog\": {
      \"name\": \"$CATALOG\",
      \"type\": \"INTERNAL\",
      \"readOnly\": false,
      \"properties\": {\"default-base-location\": \"s3://$BUCKET/\"},
      \"storageConfigInfo\": {
        \"storageType\": \"S3\",
        \"allowedLocations\": [\"s3://$BUCKET/\"],
        \"endpointInternal\": \"$S3_INTERNAL\",
        \"pathStyleAccess\": true,
        \"stsUnavailable\": true,
        \"region\": \"$REGION\"
      }
    }
  }"
  echo "✓ Catalog '$CATALOG' created on s3://$BUCKET/"
fi

# PUT grant is idempotent
curl -sS --fail-with-body -o /dev/null -X PUT -H "$AUTH" -H "$JSON" \
  "$MGMT/catalogs/$CATALOG/catalog-roles/catalog_admin/grants" \
  -d '{"type": "catalog", "privilege": "CATALOG_MANAGE_CONTENT"}'
echo "✓ catalog_admin has CATALOG_MANAGE_CONTENT"

for ns in $NAMESPACES; do
  code=$(curl -s -o /dev/null -w '%{http_code}' -X POST -H "$AUTH" -H "$JSON" \
    "$POLARIS/api/catalog/v1/$CATALOG/namespaces" -d "{\"namespace\": [\"$ns\"]}")
  case "$code" in
    200) echo "✓ Namespace $CATALOG.$ns created" ;;
    409) echo "✓ Namespace $CATALOG.$ns already exists" ;;
    *) echo "✗ Creating namespace $CATALOG.$ns failed (HTTP $code)"; exit 1 ;;
  esac
done
