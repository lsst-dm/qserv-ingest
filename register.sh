#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

INSTANCE="qserv"

REPL_CTL_POD="${INSTANCE}-repl-ctl-0"
CZAR_POD="${INSTANCE}-czar-0"

kubectl exec -it "$REPL_CTL_POD" -- /tmp/resources/register-db.sh

kubectl cp "$REPL_CTL_POD":/qserv/data/qserv/empty_desc_dc2.txt  /tmp/empty_desc_dc2.txt 
kubectl cp /tmp/empty_desc_dc2.txt "$CZAR_POD":/qserv/data/qserv/

kubectl exec -it "$CZAR_POD" -- bash -lc ". /qserv/stack/loadLSST.bash && setup mariadb -t qserv-dev && mysql --host 127.0.0.1 --port 4040 --user qsmaster -e 'select * from desc_dc2.position'"
