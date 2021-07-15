#!/bin/sh

# Ask replication system to create secondary index for Qserv data
set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. $DIR/env.sh

# Publish database
replctl-index -s -v --config $INGEST_CONFIG
