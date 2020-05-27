#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

INSTANCE="qserv"

CZAR_HOST="${INSTANCE}-czar-0.${INSTANCE}-czar"

# FIXME restart qserv to refresh xrootd cache
kubectl delete pod -l app=$INSTANCE
kubectl wait pod --for=condition=Ready -l "app=qserv"
kubectl exec -it $INSTANCE-ingest-db-0 -- bash -lc "mysql --host $CZAR_HOST --port 4040 --user qsmaster -e 'select * from desc_dc2.position'" 
