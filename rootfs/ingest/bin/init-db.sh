#!/bin/bash

# POC for loading DC2 data inside Qserv
# Based on https://confluence.lsstcorp.org/display/DM/Live+demo%3A+test+ingest+of+a+subset+of+one+track+of+the+HSC+Object+catalog

# Load python-3
DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh

set -euxo pipefail

# Create database
replctl -v --json "$QSERV_INGEST_DIR"/db.json "$BASE_URL"/ingest/database post

# Register table Position
# TODO: recreate a table
replctl -v --json "$QSERV_INGEST_DIR"/schema_position.json "$BASE_URL"/ingest/table post
