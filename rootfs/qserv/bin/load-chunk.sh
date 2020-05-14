#!/bin/bash

# POC for loading DC2 data inside Qserv
# Based on https://confluence.lsstcorp.org/display/DM/Live+demo%3A+test+ingest+of+a+subset+of+one+track+of+the+HSC+Object+catalog

# Load python-3
. /stack/loadLSST.bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh

JSON_CHUNK="/tmp/chunk.json"
JSON_TRANSACTION="/tmp/transaction.json"

CHUNK=57892
CHUNK_FILE="chunk_$CHUNK.txt"
CHUNK_FILE_OVERLAP="chunk_${CHUNK}_overlap.txt"

# Start a super-transaction
echo '{"database":"desc_dc2","auth_key":""}' | \
    curl "$BASE_URL/ingest/v1/trans" \
      -X POST \
      -H "Content-Type: application/json" \
      -d @- > "$JSON_TRANSACTION"

TRANSACTION_ID=$(cat "$JSON_TRANSACTION" | jq '.databases.desc_dc2.transactions[0].id')

echo "{\"transaction_id\":$TRANSACTION_ID,\"chunk\":$CHUNK,\"auth_key\":\"\"}" | \
    curl "$BASE_URL/ingest/v1/chunk" \
      -X POST \
      -H "Content-Type: application/json" \
      -d @- > "$JSON_CHUNK"

WORKER=$(cat "$JSON_CHUNK" | jq '.location.host' | sed 's/"//g')
PORT=$(cat "$JSON_CHUNK" | jq '.location.port' | sed 's/"//g')

cd /tmp
curl -lO https://raw.githubusercontent.com/lsst-dm/qserv-DC2/tickets/DM-24587/data/step1_1/$CHUNK_FILE
qserv-replica-file-ingest --debug --verbose FILE $WORKER $PORT $TRANSACTION_ID position P "/tmp/$CHUNK_FILE"

curl -lO https://raw.githubusercontent.com/lsst-dm/qserv-DC2/tickets/DM-24587/data/step1_1/$CHUNK_FILE_OVERLAP
qserv-replica-file-ingest --debug --verbose FILE $WORKER $PORT $TRANSACTION_ID position P "/tmp/$CHUNK_FILE_OVERLAP"

curl "$BASE_URL/ingest/v1/trans/$TRANSACTION_ID?abort=0&build-secondary-index=1" -X PUT -H "Content-Type: application/json" -d '{"auth_key":""}'
