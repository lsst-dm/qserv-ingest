#!/bin/bash
# Get the number of remaining elements in ingest chunk queue

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/../env.sh

DATABASE="cosmoDC2_v1_1_4_image_overlap"
PASSWORD=CHANGEME

kubectl exec -it $INSTANCE-ingest-db-0 -- mysql -h localhost -u root -p"$PASSWORD" -e "DELETE FROM qservIngest.task WHERE \`database\` LIKE '$DATABASE';"
time kubectl exec -it $INSTANCE-repl-ctl-0 -- curl http://localhost:$REPL_CTL_PORT/ingest/database/"$DATABASE"  \
   -X DELETE -H "Content-Type: application/json" \
   -d '{"auth_key":""}'

