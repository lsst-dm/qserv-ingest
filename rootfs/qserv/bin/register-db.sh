#!/bin/bash

# POC for loading DC2 data inside Qserv
# Based on https://confluence.lsstcorp.org/display/DM/Live+demo%3A+test+ingest+of+a+subset+of+one+track+of+the+HSC+Object+catalog

# Load python-3
. /stack/loadLSST.bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh

# Register database
replctl "$BASE_URL"/ingest/v1/database/desc_dc2 put
