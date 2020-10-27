#!/bin/sh

# Ask replication system to create secondary index for Qserv data

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh

set -euxo pipefail

# Publish database
replctl-index -s -v "$DATA_URL" "$REPL_URL"
