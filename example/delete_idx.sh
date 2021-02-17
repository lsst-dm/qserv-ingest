#!/bin/bash
# Delete an index using replication service API

set -euxo pipefail

curl 'http://localhost:$REPL_CTL_PORT/replication/sql/index'   -X DELETE -H "Content-Type: application/json"  \
    -d '{"database":"dc2_run2_1i_dr1b","table":"position","index":"objectId_uniq","auth_key":""}'
