#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. ./env.sh

kubectl apply -k $DIR/base/dev
while ! kubectl wait pod --for=condition=Ready --timeout="10s" -l "app=qserv,tier=ingest-dev"
do
  echo "Wait for Qserv ingest pods to be ready:"
  kubectl get pod -l "app=qserv,tier=ingest-dev"
  sleep 3
done
