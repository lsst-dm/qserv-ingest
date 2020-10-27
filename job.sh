#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. $DIR/env.sh

JOB="$1"

if [ -z "$OVERLAY" ]
then
    KUSTOMIZE_DIR="$DIR/manifests/base"
else
    KUSTOMIZE_DIR="$DIR/manifests/overlays/$OVERLAY"
fi

kubectl apply -k "$KUSTOMIZE_DIR/$JOB"
kubectl wait --for=condition=complete --timeout=-1s "job/ingest-$JOB"
