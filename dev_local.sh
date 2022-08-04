#!/bin/bash

# Run docker container containing DC2 ingest tools

# @author  Fabrice Jammes

set -euo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR/env.sh"

usage() {
    cat << EOD
Usage: $(basename "$0") [options]
Available options:
  -h            This message

Run docker container containing k8s management tools (helm,
kubectl, ...) and scripts.

EOD
}

# Get the options
while getopts h c ; do
    case $c in
        h) usage ; exit 0 ;;
        \?) usage ; exit 2 ;;
    esac
done
shift "$((OPTIND-1))"

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

echo -n "Check telepresence version "
if ! telepresence version | grep "Client: v2"; then
  >&2 echo "ERROR: telepresence v2 is required"
  exit 3
fi

telepresence connect

DEV_IMAGE="qserv/ingest-deps"
docker build --target ingest-deps -t "$DEV_IMAGE" "$DIR"

echo "Running in development mode"
MOUNTS="-v $DIR/rootfs/ingest:/ingest"
NAMESPACE=$(kubectl get sa -o=jsonpath='{.items[0]..metadata.namespace}')

echo "oOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoO"
echo "   Welcome in qserv-ingest developement container"
echo "   Setup for using Qserv in namespace $NAMESPACE"
echo "oOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoO"
docker run --net=host --dns-search $NAMESPACE -it $MOUNTS --rm -w "$HOME" "$DEV_IMAGE" bash
