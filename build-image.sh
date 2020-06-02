#!/bin/bash

# Create docker image containing kops tools and scripts

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. ./env.sh

for f in "ingest-telepresence" "ingest-init" "ingest-chunks" "ingest-queue" "ingest-register"
do
sed "s|<INGEST_IMAGE>|$INGEST_IMAGE|g" "$DIR/manifest/$f.yaml.tpl" \
    > "$DIR/manifest/$f.yaml"
done

docker image build --build-arg REPLICATION_IMAGE="$REPLICATION_IMAGE" --tag "$INGEST_IMAGE" "$DIR"
docker push "$INGEST_IMAGE"
