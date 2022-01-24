#!/bin/bash

# Ask Qserv replication system to register a Qserv database, prior to data ingestion
set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. $DIR/env.sh

# Register database and tables
replctl -v --config "$INGEST_CONFIG" register