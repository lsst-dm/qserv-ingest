#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

JOB="$1"
kubectl apply -k "$DIR/base/$JOB"
kubectl wait --for=condition=complete --timeout=-1s "job/ingest-$JOB"
