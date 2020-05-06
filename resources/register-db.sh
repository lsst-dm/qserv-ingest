#!/bin/bash

# POC for loading DC2 data inside Qserv
# Based on https://confluence.lsstcorp.org/display/DM/Live+demo%3A+test+ingest+of+a+subset+of+one+track+of+the+HSC+Object+catalog

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh

mkdir -p /qserv/data/qserv/

TRANSACTION_ID=$(cat "$JSON_TRANSACTION" | $JQ '.databases.desc_dc2.transactions[0].id')
curl "$BASE_URL/ingest/v1/trans/$TRANSACTION_ID?abort=0&build-secondary-index=1" -X PUT -H "Content-Type: application/json" -d '{"auth_key":""}'
curl "$BASE_URL/ingest/v1/database/desc_dc2" -X PUT -H "Content-Type: application/json" -d '{"auth_key":""}'
