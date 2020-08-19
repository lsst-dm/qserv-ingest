#!/bin/bash
# Get the number of remaining elements in ingest chunk queue

set -euxo pipefail


PASSWORD=CHANGEME
kubectl exec -it qserv-ingest-db-0 -- mysql -h localhost -u root -p"$PASSWORD" -e "select count(*) from qservIngest.task;"
