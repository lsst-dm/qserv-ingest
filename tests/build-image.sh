#!/bin/bash

# Create docker image containing qserv dataserver

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.build.sh

docker image build --build-arg BASE_IMAGE="$BASE_IMAGE" --tag "$IMAGE_TAG" "$DIR"
docker push "$IMAGE_TAG"
docker tag "$IMAGE_TAG" "$IMAGE:latest"
docker push "$IMAGE:latest"

