#!/bin/sh

# Ask Qserv replication system to register a Qserv database, prior to data ingestion

# Load python-3
DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh

set -euxo pipefail

# Register database and table
replctl-register -v "$REPL_URL" "$DATA_URL"
