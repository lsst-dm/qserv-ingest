#!/bin/bash

# Launch ingest input dataserver
# used by integration tests

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
kubectl apply -k $DIR/manifests/

TMP_DIR=$(mktemp -d)
$DIR/get-testdata.sh  "$TMP_DIR"

DATASERVER_POD=$(kubectl get pod -l app=qserv,tier=dataserver -o jsonpath="{.items[0].metadata.name}")
kubectl cp "$TMP_DIR"/datasets.tgz "$DATASERVER_POD":/tmp
kubectl exec -it "$DATASERVER_POD" -- tar zxvf /tmp/datasets.tgz --directory /usr/share/nginx/html
kubectl cp "$DIR"/datasets "$DATASERVER_POD":/usr/share/nginx/html
