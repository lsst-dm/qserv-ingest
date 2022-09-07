#!/bin/bash

# Install qserv-operator and Qserv

# @author Fabrice Jammes SLAC/IN2P3

set -euxo pipefail

OPERATOR_VERSION="main"
OPERATOR_DIR="/tmp/qserv-operator"
if [ -d "$OPERATOR_DIR" ]; then
  rm -rf "$OPERATOR_DIR"
fi

git clone https://github.com/lsst/qserv-operator --branch "$OPERATOR_VERSION" \
  --single-branch --depth=1 "$OPERATOR_DIR"
"$OPERATOR_DIR"/prereq-install.sh
kubectl apply -f "$OPERATOR_DIR"/manifests/operator.yaml
"$OPERATOR_DIR"/tests/tools/wait-operator-ready.sh
kubectl apply -k "$OPERATOR_DIR"/manifests/base
"$OPERATOR_DIR"/tests/tools/wait-qserv-ready.sh
# Wait for replication controller initialization
# see https://lsstc.slack.com/archives/G2JPZ3GC8/p1658864074354089
sleep 20
