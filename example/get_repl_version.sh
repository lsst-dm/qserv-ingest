#!/bin/bash
# List all super-transactions for a database

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/../env.sh


PASSWORD=""
kubectl exec -it qserv-repl-ctl-0 -- curl "http://qserv-repl-ctl:$REPL_CTL_PORT/meta/version" -X GET  -H "Content-Type: application/json" -d "{\"auth_key\":\"$PASSWORD\"}"

