#!/usr/bin/env sh
set -eu

API_PORT="${PORT:-3000}"
OPENAPI_URL="${OPENAPI_URL:-http://localhost:${API_PORT}/api-docs/openapi.json}"
OUTPUT_DIR="${OUTPUT_DIR:-postman}"
OPENAPI_FILE="${OUTPUT_DIR}/assetar.openapi.json"
COLLECTION_FILE="${OUTPUT_DIR}/Assetar.postman_collection.json"

mkdir -p "$OUTPUT_DIR"

echo "Fetching OpenAPI spec from ${OPENAPI_URL}"
if ! curl -fsS "$OPENAPI_URL" -o "$OPENAPI_FILE"; then
  echo "Failed to fetch OpenAPI spec from ${OPENAPI_URL}" >&2
  echo "Start the Rust backend first, or override the source:" >&2
  echo "  OPENAPI_URL=https://your-api-host/api-docs/openapi.json npm run postman:collection" >&2
  echo "  PORT=3000 npm run postman:collection" >&2
  exit 1
fi

echo "Generating Postman collection"
if command -v openapi2postmanv2 >/dev/null 2>&1; then
  openapi2postmanv2 \
    -s "$OPENAPI_FILE" \
    -o "$COLLECTION_FILE" \
    -p \
    -O folderStrategy=Tags,includeAuthInfoInExample=false
else
  npx -p openapi-to-postmanv2 openapi2postmanv2 \
    -s "$OPENAPI_FILE" \
    -o "$COLLECTION_FILE" \
    -p \
    -O folderStrategy=Tags,includeAuthInfoInExample=false
fi

echo "Wrote ${COLLECTION_FILE}"
