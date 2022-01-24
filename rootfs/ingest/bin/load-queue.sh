#!/bin/bash

# Launch parallel ingest tasks based on Qserv replication system
set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. $DIR/env.sh

replctl -v --config "$INGEST_CONFIG" queue
