#!/bin/bash

# Create docker image containing kops tools and scripts

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. ./env.sh

for f in "dev" "ingest" "init" "publish"
do
sed "s|<INGEST_IMAGE>|$INGEST_IMAGE|g" "$DIR/base/$f/$f.yaml.tpl" \
    > "$DIR/base/$f/$f.yaml"
done

docker build --build-arg REPLICATION_IMAGE="$REPLICATION_IMAGE" --target ingest-deps -t "$INGEST_DEPS_IMAGE" "$DIR"
docker image build --build-arg REPLICATION_IMAGE="$REPLICATION_IMAGE" --tag "$INGEST_IMAGE" "$DIR"
docker push "$INGEST_IMAGE"
docker push "$INGEST_DEPS_IMAGE"
