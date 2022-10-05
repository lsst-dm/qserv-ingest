#!/bin/bash

# Install qserv-operator and Qserv

# @author Fabrice Jammes SLAC/IN2P3

set -euxo pipefail

OPERATOR_DIR="/tmp/qserv-operator"
if [ -d "$OPERATOR_DIR" ]; then
  rm -rf "$OPERATOR_DIR"
fi

REPO_URL="https://github.com/lsst/qserv-operator"

GIT_BRANCH=$(git rev-parse --abbrev-ref HEAD)
BRANCH=${GHA_BRANCH_NAME:-$GIT_BRANCH}
# Retrieve same qserv-operator branch if it exists, else use qserv-operator main branch
if git ls-remote --exit-code --heads "$REPO_URL" "$BRANCH"
then
    OPERATOR_VERSION="$BRANCH"
else
    OPERATOR_VERSION="main"
fi

git clone "$REPO_URL" --branch "$OPERATOR_VERSION" \
  --single-branch --depth=1 "$OPERATOR_DIR"
"$OPERATOR_DIR"/prereq-install.sh
kubectl apply -f "$OPERATOR_DIR"/manifests/operator.yaml
"$OPERATOR_DIR"/tests/tools/wait-operator-ready.sh
kubectl apply -k "$OPERATOR_DIR"/manifests/base
"$OPERATOR_DIR"/tests/tools/wait-qserv-ready.sh
# Wait for replication controller initialization
# see https://lsstc.slack.com/archives/G2JPZ3GC8/p1658864074354089
sleep 20
