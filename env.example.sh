# Set variable below to empty string in order to install
# current development version
INGEST_RELEASE='2022.9.1-rc1'
TAG=${INGEST_RELEASE:-$(git -C $DIR describe --dirty --always)}

# Select dataset to load
# related directory must exists inside ./manifests/
OVERLAY=base
# OVERLAY="case01"
# OVERLAY="case03"
# OVERLAY="in2p3-skysim5000"
# OVERLAY="in2p3-dc2_dr6_object_v2"
# OVERLAY="dc2-errors"
# OVERLAY="in2p3-dp0.2"

# Image version created by build procedure
INGEST_IMAGE="qserv/ingest:$TAG"
INGEST_DEPS_IMAGE="qserv/ingest-deps:latest"
INSTANCE="qserv"

REPL_CTL_PORT="8080"
