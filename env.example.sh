GIT_HASH="$(git -C $DIR describe --dirty --always)"
TAG=${INGEST_VERSION:-${GIT_HASH}}

BASE_IMAGE="python:3.9.0-alpine3.12"
# Image version created by build procedure
INGEST_IMAGE="qserv/ingest:$TAG"
INGEST_DEPS_IMAGE="qserv/ingest-deps:latest"
INSTANCE="qserv"
OVERLAY="base"
#OVERLAY="in2p3-dc2_dr6_object_v2"
#OVERLAY="dc2-errors"

REPL_CTL_PORT="8080"
