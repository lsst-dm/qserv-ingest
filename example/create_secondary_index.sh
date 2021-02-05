#!/bin/bash
# Get the number of remaining elements in ingest chunk queue

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/../env.sh

DATABASE="cosmoDC2_v1_1_4_image_overlap"

time kubectl exec -it qserv-repl-ctl-0 -- curl http://localhost:25080/ingest/index/secondary \
   -X POST -H "Content-Type: application/json" \
   -d '{"database":"'"$DATABASE"'","allow_for_published":1,"rebuild":1,"local":1,"auth_key":""}'

