#!/bin/sh

# Launch parallel ingest tasks based on Qserv replication system

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh

set -euxo pipefail

replctl-ingest -v "$REPL_URL" mysql://qsingest:@qserv-ingest-db-0.qserv-ingest-db:3306/qservIngest "$DATA_URL"
