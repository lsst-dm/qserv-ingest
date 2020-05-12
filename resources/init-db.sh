#!/bin/bash

# POC for loading DC2 data inside Qserv
# Based on https://confluence.lsstcorp.org/display/DM/Live+demo%3A+test+ingest+of+a+subset+of+one+track+of+the+HSC+Object+catalog

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh

curl https://stedolan.github.io/jq/download/linux64/jq > $JQ
chmod +x "$JQ"

# Create database
curl "$BASE_URL/ingest/v1/database" \
  -X POST \
  -H "Content-Type: application/json" \
  -d "@$DIR/db.json"

# Register table Position
# TODO: recreate a table
curl "$BASE_URL/ingest/v1/table" \
  -X POST \
  -H "Content-Type: application/json" \
  -d "@$DIR/schema_position.json"
