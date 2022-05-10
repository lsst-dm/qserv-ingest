#!/bin/bash

# Launch unit tests 

# @author  Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. ./env.build.sh

# Build ingest image
$DIR/build.sh

# Launch unit tests
docker run -it "$INGEST_IMAGE" /ingest/bin/pytest.sh
