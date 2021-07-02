GIT_HASH="$(git -C $DIR describe --dirty --always)"
TAG="$GIT_HASH"

BASE_IMAGE="python:3.9.0-alpine3.12"
INGEST_DEPS_IMAGE="qserv/ingest-deps:latest"

# Image version created by build procedure
INGEST_IMAGE="qserv/ingest:$TAG"
