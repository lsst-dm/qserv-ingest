#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

INSTANCE="qserv"

REPL_CTL_POD="${INSTANCE}-repl-ctl-0"

kubectl exec -it "$REPL_CTL_POD" -- /tmp/resources/register-db.sh
