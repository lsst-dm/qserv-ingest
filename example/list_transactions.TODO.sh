#!/bin/bash
# List all super-transactions for a database

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/../env.sh

# DATABASE="dc2_object_run2_2i_dr6_wfd"
DATABASE="cosmoDC2_v1_1_4_image_overlap"

PASSWORD=""
kubectl exec -it qserv-repl-ctl-0 -- curl "http://qserv-repl-ctl:$REPL_CTL_PORT/ingest/trans/?database=$DATABASE" -X GET  -H "Content-Type: application/json" -d "{\"auth_key\":\"$PASSWORD\"}"

URL="ingest/trans/?family=&all_databases=0&is_published=0&contrib=1&contrib_long=0"
kubectl exec -it qserv-repl-ctl-0 -- curl "http://qserv-repl-ctl:$REPL_CTL_PORT/$URL" -X GET  -H "Content-Type: application/json" -d "{\"auth_key\":\"$PASSWORD\"}"

