#!/bin/bash

# Install qserv-operator and Qserv

# @author Fabrice Jammes SLAC/IN2P3

set -euxo pipefail

OPERATOR_VERSION="2022.6.2-rc1-6-gef0de68"
OPERATOR_DIR="/tmp/qserv-operator"
if [ -d "$OPERATOR_DIR" ]; then
  rm -rf "$OPERATOR_DIR"
fi

git clone https://github.com/lsst/qserv-operator "$OPERATOR_DIR"
git -C "$OPERATOR_DIR" checkout "$OPERATOR_VERSION"
"$OPERATOR_DIR"/prereq-install.sh
kubectl apply -f "$OPERATOR_DIR"/manifests/operator.yaml
"$OPERATOR_DIR"/tests/tools/wait-operator-ready.sh
kubectl apply -k "$OPERATOR_DIR"/manifests/base
"$OPERATOR_DIR"/tests/tools/wait-qserv-ready.sh
