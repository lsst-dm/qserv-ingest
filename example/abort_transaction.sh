#!/bin/bash
# List all super-transactions for a database

set -euxo pipefail

TRANS_ID=19
PASSWORD=""
kubectl exec -it qserv-repl-ctl-0 -- curl "http://qserv-repl-ctl-0.qserv-repl-ctl:25080/ingest/trans/$TRANS_ID?abort=1" -X PUT  -H "Content-Type: application/json" -d "{\"auth_key\":\"$PASSWORD\"}"
