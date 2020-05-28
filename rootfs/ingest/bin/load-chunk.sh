#!/bin/bash

# POC for loading DC2 data inside Qserv
# Based on https://confluence.lsstcorp.org/display/DM/Live+demo%3A+test+ingest+of+a+subset+of+one+track+of+the+HSC+Object+catalog

# Enable python3 and mysqlclient
. /stack/loadLSST.bash
setup db
setup python_mysqlclient
setup sqlalchemy

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh

DATABASE="desc_dc2"

CHUNK=57892
CHUNK_FILE="chunk_$CHUNK.txt"
CHUNK_FILE_OVERLAP="chunk_${CHUNK}_overlap.txt"

CHUNK_PATH="/tmp"

cd "$CHUNK_PATH"
curl -lO https://raw.githubusercontent.com/lsst-dm/qserv-DC2/tickets/DM-24587/data/step1_1/$CHUNK_FILE
curl -lO https://raw.githubusercontent.com/lsst-dm/qserv-DC2/tickets/DM-24587/data/step1_1/$CHUNK_FILE_OVERLAP

replctl-task -v "$BASE_URL" "$DATABASE" mysql://qsingest:@qserv-ingest-db-0.qserv-ingest-db:3306/qservIngest
