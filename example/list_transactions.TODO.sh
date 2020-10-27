#!/bin/bash
# List all super-transactions for a database

set -euxo pipefail

DATABASE="dc2_object_run2_2i_dr6_wfd"
#DATABASE="dc2_run2_1i_dr1b"
PASSWORD=""
kubectl exec -it qserv-repl-ctl-0 -- curl "http://qserv-repl-ctl-0.qserv-repl-ctl:25080/ingest/trans/?database=$DATABASE" -X GET  -H "Content-Type: application/json" -d "{\"auth_key\":\"$PASSWORD\"}"
