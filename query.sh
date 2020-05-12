#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

INSTANCE="qserv"

CZAR_POD="${INSTANCE}-czar-0"

kubectl exec -it "$CZAR_POD" -- bash -lc ". /qserv/stack/loadLSST.bash && setup mariadb -t qserv-dev && mysql --host 127.0.0.1 --port 4040 --user qsmaster -e 'select * from desc_dc2.position'"
