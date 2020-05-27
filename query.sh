#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

INSTANCE="qserv"

CZAR_POD="${INSTANCE}-czar-0.${INSTANCE}-czar"

# FIXME restart qserv to refresh xrootd cache
kubectl delete pod -l app=$INSTANCE
kubectl exec -it qserv-ingest -- bash -lc ". /stack/loadLSST.bash && \
    setup mariadb -t qserv-dev && \
    mysql --host $CZAR_HOST --port 4040 --user qsmaster -e 'select * from desc_dc2.position'" 
