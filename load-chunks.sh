#!/bin/bash

set -euxo pipefail

INSTANCE="qserv"
INGEST_POD="qserv-ingest"
INGEST_IMAGE="qserv/replica:tools-w.2018.16-1307-g3b2335a-dirty"

DIR=$(cd "$(dirname "$0")"; pwd -P)

kubectl delete pod -l "app=qserv,tier=ingest,instance=$INSTANCE" --now
kubectl run --image="$INGEST_IMAGE" --restart=Never "$INGEST_POD" --command sleep -- 3600
kubectl label pod "$INGEST_POD" "app=qserv" "tier=ingest" "instance=$INSTANCE"
while ! kubectl wait pod --for=condition=Ready --timeout="10s" -l "app=qserv,tier=ingest,instance=$INSTANCE"
do
  echo "Wait for Qserv ingest pod to be ready:"
  kubectl get pod -l "app=qserv,tier=ingest,instance=$INSTANCE"
  sleep 3
done

kubectl cp "$DIR/resources" "$INGEST_POD":/tmp
kubectl exec -it "$INGEST_POD" -- /tmp/resources/load-chunk.sh
