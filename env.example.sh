GIT_HASH="$(git describe --dirty --always)"
TAG=${OP_VERSION:-${GIT_HASH}}

# Image version created by build procedure
REPLICATION_IMAGE="qserv/qserv:tickets_DM-26034"
INGEST_IMAGE="qserv/ingest:$TAG"
INGEST_DEPS_IMAGE="qserv/ingest-deps:latest"
INSTANCE="qserv"
OVERLAY=""
