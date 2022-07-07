# Set variable below to empty string in order to install
# current development version
INGEST_RELEASE='2022.1.1-rc1'

# Select dataset to load
# related directory must exists inside ./manifests/
OVERLAY=in2p3-dp0.2

if [ -n "$INGEST_RELEASE" ];
then
    TAG="$INGEST_RELEASE"
else
    GIT_HASH="$(git -C $DIR describe --dirty --always)"
    TAG="$GIT_HASH"
fi

# Image version created by build procedure
INGEST_IMAGE="qserv/ingest:$TAG"
INGEST_DEPS_IMAGE="qserv/ingest-deps:latest"
INSTANCE="qserv"

REPL_CTL_PORT="8080"
