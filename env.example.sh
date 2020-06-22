GIT_HASH="$(git describe --dirty --always)"
TAG=${OP_VERSION:-${GIT_HASH}}

# Image version created by build procedure
REPLICATION_IMAGE="qserv/replica:tools-w.2018.16-1319-g0f0be83"
INGEST_IMAGE="qserv/ingest:$TAG"
INGEST_DEPS_IMAGE="qserv/ingest-deps:latest"
INSTANCE="qserv"
OVERLAY=""
