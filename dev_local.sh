#!/bin/bash

# Run docker container containing DC2 ingest tools 

# @author  Fabrice Jammes

set -euxo pipefail

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

echo "Running in development mode"
MOUNTS="-v $DIR/rootfs/ingest:/ingest"

docker pull "$INGEST_IMAGE"
echo "oOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoO"
echo "   Welcome in DC2 ingest container"
echo "oOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoOoO"
telepresence --swap-deployment ingest-dev \
    --docker-run -it \
    $MOUNTS --rm \
    -w $HOME \
    "$INGEST_DEPS_IMAGE"
