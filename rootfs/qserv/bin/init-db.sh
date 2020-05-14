#!/bin/bash

# POC for loading DC2 data inside Qserv
# Based on https://confluence.lsstcorp.org/display/DM/Live+demo%3A+test+ingest+of+a+subset+of+one+track+of+the+HSC+Object+catalog

# Load python-3
. /stack/loadLSST.bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh

# Create replication manager credentials
touch ~/.lsst/qserv

# Create database
qingest.py --json "$QSERV_INGEST_DIR"/db.json "$BASE_URL"/ingest/v1/database post

# Register table Position
# TODO: recreate a table
qingest.py --json "$QSERV_INGEST_DIR"/schema_position.json "$BASE_URL"/ingest/v1/table post
