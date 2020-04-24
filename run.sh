#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

kubectl cp "$DIR/resources" qserv-dev-repl-ctl-0:/tmp
kubectl exec -it qserv-dev-repl-ctl-0 -- /tmp/resources/create-db.sh
