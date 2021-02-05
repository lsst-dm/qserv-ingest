#!/bin/sh

# Launch parallel ingest tasks based on Qserv replication system
set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh

load-queue -v "$data_url" "$QUEUE_URL"