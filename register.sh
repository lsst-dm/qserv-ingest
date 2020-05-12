#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

INSTANCE="qserv"

REPL_CTL_POD="${INSTANCE}-repl-ctl-0"
CZAR_POD="${INSTANCE}-czar-0"

kubectl exec -it "$REPL_CTL_POD" -- /tmp/resources/register-db.sh

kubectl cp "$REPL_CTL_POD":/qserv/data/qserv/empty_desc_dc2.txt  /tmp/empty_desc_dc2.txt 
kubectl cp /tmp/empty_desc_dc2.txt "$CZAR_POD":/qserv/data/qserv/
