#!/bin/bash

# POC for loading DC2 data inside Qserv
# Based on https://confluence.lsstcorp.org/display/DM/Live+demo%3A+test+ingest+of+a+subset+of+one+track+of+the+HSC+Object+catalog

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

BASE_URL="http://localhost:8080"

curl "$BASE_URL/ingest/v1/database" \
  -X POST \
  -H "Content-Type: application/json" \
  -d "@$DIR/db.json" || echo "WARN: failed to create db"

curl "$BASE_URL/ingest/v1/table" \
  -X POST \
  -H "Content-Type: application/json" \
  -d "@$DIR/schema_object.json" || echo "WARN: failed to create object table"

echo '{"database":"desc_dc2"}' | \
    curl "$BASE_URL/ingest/v1/trans" \
      -X POST \
      -H "Content-Type: application/json" \
      -d @-

TRANSACTION_ID=37

curl "$BASE_URL/ingest/v1/trans/$TRANSACTION_ID?abort=0&build-secondary-index=1" -X PUT