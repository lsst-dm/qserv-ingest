#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

JOB="ingest-$1"
kubectl apply -f "$DIR/manifest/$JOB.yaml"
kubectl wait --for=condition=complete --timeout=-1s "job/$JOB"
