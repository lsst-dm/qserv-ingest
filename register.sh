#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh
INGEST_POD=$(kubectl get pods -l tier=ingest -o jsonpath="{.items[0].metadata.name}")

CZAR_POD="${INSTANCE}-czar-0"
REPL_CTL_POD="${INSTANCE}-repl-ctl-0"

kubectl exec -it "$INGEST_POD" -- register-db.sh

kubectl cp "$REPL_CTL_POD":/qserv/data/qserv/empty_desc_dc2.txt  /tmp/empty_desc_dc2.txt 
kubectl cp /tmp/empty_desc_dc2.txt "$CZAR_POD":/qserv/data/qserv/
