#!/bin/bash
# List all super-transactions for a database

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/../env.sh

DB_USER="qsingest"

POD="$INSTANCE-ingest-db-0"
kubectl exec -it "$POD" -- mysql -h "$POD" -u "$DB_USER" -e 'SELECT DISTINCT `database` FROM qservIngest.contribfile_queue'
