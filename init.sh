#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. ./env.sh
INGEST_POD=$(kubectl get pods -l tier=ingest -o jsonpath="{.items[0].metadata.name}")

kubectl exec -it "$INGEST_POD" -- init-db.sh
