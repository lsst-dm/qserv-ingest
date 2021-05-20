#!/bin/bash
# Delete a super-transactions for a database

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/../env.sh

TRANS_ID=8
PASSWORD=""
kubectl exec -it qserv-repl-ctl-0 -- curl "http://qserv-repl-ctl-0.qserv-repl-ctl:$REPL_CTL_PORT/ingest/trans/$TRANS_ID?abort=1" \
  -X PUT  -H "Content-Type: application/json" -d "{\"auth_key\":\"$PASSWORD\"}"
