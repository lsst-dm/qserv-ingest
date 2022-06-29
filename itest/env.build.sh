TAG="$(git describe --dirty --always)"

BASE_IMAGE="nginxinc/nginx-unprivileged:1.20"
# Image version created by build procedure
IMAGE="qserv/dataserver"
IMAGE_TAG="$IMAGE:$TAG"
