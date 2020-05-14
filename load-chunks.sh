#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. ./env.sh

kubectl exec -it "$INGEST_POD" -- load-chunk.sh
