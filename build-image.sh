#!/bin/bash

# Create docker image containing qserv-ingest 

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. ./env.sh

docker image build --build-arg BASE_IMAGE="$BASE_IMAGE" --tag "$INGEST_IMAGE" "$DIR"
