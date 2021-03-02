#!/bin/bash

# Launch ingest input dataserver
# used by integration tests

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
kubectl apply -f $DIR/dataserver.yaml
