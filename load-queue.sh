#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

JOB="ingest-queue"
kubectl apply -f "$DIR/manifest/$JOB.yaml"
kubectl wait --for=condition=complete "job/$JOB"
