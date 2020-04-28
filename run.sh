#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

INSTANCE="qserv"

REPL_CTL_POD="${INSTANCE}-repl-ctl-0"

kubectl cp "$DIR/resources" "$REPL_CTL_POD":/tmp
kubectl exec -it "$REPL_CTL_POD" -- /tmp/resources/load-db.sh
# TODO load script on correct worker
#./load.sh
#kubectl exec -it "$REPL_CTL_POD" -- /tmp/resources/register-db.sh
