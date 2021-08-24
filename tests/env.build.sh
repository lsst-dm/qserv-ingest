TAG="$(git describe --dirty --always)"

BASE_IMAGE="nginx:1.19.6-alpine"
# Image version created by build procedure
IMAGE="qserv/dataserver"
IMAGE_TAG="$IMAGE:$TAG"
