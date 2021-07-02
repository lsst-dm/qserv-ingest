#!/bin/bash

# Create docker image containing qserv-ingest 

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. ./env.build.sh

docker build --build-arg BASE_IMAGE="$BASE_IMAGE" --target ingest-deps -t "$INGEST_DEPS_IMAGE" "$DIR"
docker image build --build-arg BASE_IMAGE="$BASE_IMAGE" --tag "$INGEST_IMAGE" "$DIR"
docker push "$INGEST_IMAGE"
docker push "$INGEST_DEPS_IMAGE"
