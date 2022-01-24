#!/bin/bash

# Create docker image containing qserv-ingest

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. ./env.build.sh

# Build ingest image
docker image build --tag "$INGEST_IMAGE" "$DIR"
