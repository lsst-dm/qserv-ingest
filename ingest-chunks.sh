#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

kubectl apply -f $DIR/manifest/ingest-chunks.yaml
