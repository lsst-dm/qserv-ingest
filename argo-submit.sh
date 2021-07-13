#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. $DIR/env.sh

kubectl apply -k $DIR/manifests/$OVERLAY/configmap
argo submit --serviceaccount=argo-workflow -p image="$INGEST_IMAGE" --entrypoint main -vvv $DIR/manifests/workflow.yaml
