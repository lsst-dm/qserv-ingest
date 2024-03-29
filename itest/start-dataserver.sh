#!/bin/bash

# Launch ingest input dataserver
# used by integration tests

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
kubectl apply -k $DIR/manifests/
kubectl wait --timeout=400s --for=condition=available deploy dataserver
DATASERVER_POD=$(kubectl get pod -l app=qserv,tier=dataserver -o jsonpath="{.items[0].metadata.name}")
kubectl describe pod "$DATASERVER_POD"
kubectl wait --timeout=60s --for=condition=Ready pods "$DATASERVER_POD"
kubectl cp "$DIR"/datasets "$DATASERVER_POD":/usr/share/nginx/html
