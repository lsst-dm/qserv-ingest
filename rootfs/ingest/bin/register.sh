#!/bin/sh

# Ask Qserv replication system to register a Qserv database, prior to data ingestion
set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. $DIR/env.sh

# Register database and tables
replctl-register -v --config "$INGEST_CONFIG"