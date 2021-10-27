# Set variable below to empty string in order to install
# current development version
INGEST_RELEASE='2021.10.1-rc1'

# Select dataset to load
# related directory must exists inside ./manifests/
OVERLAY=base
#OVERLAY="in2p3-skysim5000"
#OVERLAY="in2p3-dc2_dr6_object_v2"
#OVERLAY="dc2-errors"

if [ -n "$INGEST_RELEASE" ];
then
    TAG="$INGEST_RELEASE"
else
    GIT_HASH="$(git -C $DIR describe --dirty --always)"
    TAG="$GIT_HASH"
fi

BASE_IMAGE="python:3.9.0-alpine3.12"
# Image version created by build procedure
INGEST_IMAGE="qserv/ingest:$TAG"
INGEST_DEPS_IMAGE="qserv/ingest-deps:latest"
INSTANCE="qserv"

REPL_CTL_PORT="8080"
