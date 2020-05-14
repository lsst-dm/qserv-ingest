#!/bin/bash

# Create docker image containing kops tools and scripts

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. ./env.sh

# CACHE_OPT="--no-cache"

docker image build --build-arg REPLICATION_IMAGE="$REPLICATION_IMAGE" --tag "$INGEST_IMAGE" "$DIR"
docker push "$INGEST_IMAGE"
