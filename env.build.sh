GIT_HASH="$(git -C $DIR describe --dirty --always)"
TAG="$GIT_HASH"

INGEST_DEPS_IMAGE="qserv/ingest-deps:latest"

# Image version created by build procedure
INGEST_IMAGE="qserv/ingest:$TAG"
