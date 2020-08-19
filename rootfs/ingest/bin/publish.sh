#!/bin/bash

# Ask Qserv replication system to publish a Qserv database

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh

set -euxo pipefail

# Publish database
replctl-publish -v "$REPL_URL" "$DATA_URL"
