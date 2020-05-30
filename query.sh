#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh

CZAR_HOST="${INSTANCE}-czar-0.${INSTANCE}-czar"

kubectl exec -it $INSTANCE-ingest-db-0 -- \
    bash -lc "mysql --host $CZAR_HOST --port 4040 --user qsmaster -e 'SELECT * FROM desc_dc2.position LIMIT 10'"
