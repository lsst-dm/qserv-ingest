#!/bin/sh

# Ask replication system to create tables indexes for Qserv data
set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh

# Publish database
replctl-index -v "$DATA_URL" "$REPL_URL"
