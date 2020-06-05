#!/bin/bash

# Copy emptychunk file from replication controller to czar

# @author Fabrice Jammes

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh

CZAR_POD="${INSTANCE}-czar-0"
REPL_CTL_POD="${INSTANCE}-repl-ctl-0"

EMPTYCHUNK_FILE="empty_dc2_run2_1i_dr1b.txt"
kubectl cp "$REPL_CTL_POD":/qserv/data/qserv/"$EMPTYCHUNK_FILE" /tmp/"$EMPTYCHUNK_FILE"
kubectl cp /tmp/"$EMPTYCHUNK_FILE" "$CZAR_POD":/qserv/data/qserv/"$EMPTYCHUNK_FILE"
