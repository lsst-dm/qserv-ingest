GIT_HASH="$(git describe --dirty --always)"
TAG=${OP_VERSION:-${GIT_HASH}}

# Image version created by build procedure
REPLICATION_IMAGE="qserv/replica:tools-w.2018.16-1307-g3b2335a-dirty"
INGEST_IMAGE="qserv/qserv-ingest:$TAG"
INSTANCE="qserv"

