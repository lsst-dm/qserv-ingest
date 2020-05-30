#!/bin/bash

# Create docker image containing kops tools and scripts

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. ./env-build.sh

sed "s|<INGEST_IMAGE>|$INGEST_IMAGE|g" "$DIR/manifest/ingest.yaml.tpl" \
    > "$DIR/manifest/ingest.yaml"
docker image build --build-arg REPLICATION_IMAGE="$REPLICATION_IMAGE" --tag "$INGEST_IMAGE" "$DIR"
docker push "$INGEST_IMAGE"
