#!/bin/bash

# POC for loading DC2 data inside Qserv
# Based on https://confluence.lsstcorp.org/display/DM/Live+demo%3A+test+ingest+of+a+subset+of+one+track+of+the+HSC+Object+catalog

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

JQ="/tmp/jq"
curl https://stedolan.github.io/jq/download/linux64/jq > $JQ
chmod +x "$JQ"

JSON_TRANSACTION="/tmp/transaction.json"

PORT="25080"
BASE_URL="http://localhost:${PORT}"

# Create database
curl "$BASE_URL/ingest/v1/database" \
  -X POST \
  -H "Content-Type: application/json" \
  -d "@$DIR/db.json" || echo "WARN: failed to create db"

# Register table Position
curl "$BASE_URL/ingest/v1/table" \
  -X POST \
  -H "Content-Type: application/json" \
  -d "@$DIR/schema_object.json" || echo "WARN: failed to create object table"

# Start a super-transaction
echo '{"database":"desc_dc2"}' | \
    curl "$BASE_URL/ingest/v1/trans" \
      -X POST \
      -H "Content-Type: application/json" \
      -d @- > "$JSON_TRANSACTION"


TRANSACTION_ID=$(cat "$JSON_TRANSACTION" | $JQ '.databases.desc_dc2.transactions[0].id')
CHUNK=57892

echo "{\"transaction_id\":$TRANSACTION_ID,\"chunk\":$CHUNK}" | \
    curl "$BASE_URL/ingest/v1/chunk" \
      -X POST \
      -H "Content-Type: application/json" \
      -d @-

curl "$BASE_URL/ingest/v1/trans/$TRANSACTION_ID?abort=0&build-secondary-index=1" -X PUT
