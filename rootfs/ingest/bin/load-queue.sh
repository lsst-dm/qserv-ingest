#!/bin/bash

# POC for loading DC2 data inside Qserv
# Based on https://confluence.lsstcorp.org/display/DM/Live+demo%3A+test+ingest+of+a+subset+of+one+track+of+the+HSC+Object+catalog

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh

set -euxo pipefail

load-queue -v "$CHUNKS_URL" mysql://qsingest:@qserv-ingest-db-0.qserv-ingest-db:3306/qservIngest