#!/bin/sh

# Launch parallel ingest tasks based on Qserv replication system

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh

set -euxo pipefail

SERVERS_OPT=''
if [ -e /config-data-url/servers.json ]; then
    SERVERS_OPT="-s /config-data-url/servers.json"
fi

replctl-ingest -v "$REPL_URL" mysql://qsingest:@qserv-ingest-db-0.qserv-ingest-db:3306/qservIngest \
    $SERVERS_OPT "$DATA_URL"
