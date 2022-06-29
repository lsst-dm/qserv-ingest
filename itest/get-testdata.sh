#!/bin/bash

set -euxo pipefail

QSERV_SRC_DIR=$HOME/src/qserv

GID=$(id -g)

usage() {
  cat << EOD

Usage: `basename $0` [options] output_path

  Available options:
    -h          this message

  Prepare qserv integration test datasets for qserv-ingest integration tests
  output datasets are produced in <output_path>

EOD
}

# get the options
while getopts h c ; do
    case $c in
        h) usage ; exit 0 ;;
        \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

if [ $# -ne 1 ] ; then
    usage
    exit 2
fi

OUT_DIR=$1

# Clone qserv source code
if [ ! -d "$QSERV_SRC_DIR" ]; then
  git clone https://github.com/lsst/qserv "$QSERV_SRC_DIR"
fi

cd "$QSERV_SRC_DIR"
git checkout "2022.6.2-rc1-19-g5e52f0365"
git submodule update --init

# Install qserv cli
export PATH="${PATH}:${QSERV_SRC_DIR}/admin/local/cli"

NETWORK_NAME="${USER}_default"
NETWORK_ID=$(docker network ls -f name="^${NETWORK_NAME}$" -q)
if [ -z "${NETWORK_ID}" ]; then
  docker network create ${USER}_default
fi

qserv build-images --pull-image --group docker_outer

USER_ID=$(id -u)
USER_OPT=""
if [ "$USER_ID" -eq 1000 ]; then
  USER_OPT="--user=qserv"
fi
qserv build -j8 $USER_OPT --unit-test
qserv prepare-data


CONTAINER_NAME="${USER}_testdata"

mkdir -p "$OUT_DIR"
cd -
docker run --name ${CONTAINER_NAME} --mount src=${USER}_itest_exe,dst=/qserv/data/,type=volume \
  --mount src="$OUT_DIR",dst=/home/ubuntu,type=bind --rm --network=${NETWORK_NAME} \
  -- ubuntu sh -c "cp -r /qserv/data/datasets.tgz /home/ubuntu/ && chown $UID:$GID /home/ubuntu/datasets.tgz"

echo "Dataset archive is available in $OUT_DIR/datasets.tgz"
echo "Unzip it inside qserv-ingest/itest/datasets to use it"
