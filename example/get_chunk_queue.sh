#!/bin/bash
# Get the number of remaining elements in ingest chunk queue

set -euxo pipefail


PASSWORD=CHANGEME
echo "NOT STAGED"
kubectl exec -it qserv-ingest-db-0 -- mysql -h localhost -u root -p"$PASSWORD" -e 'select count(*) from qservIngest.chunkfile_queue WHERE locking_pod is NULL;'
echo "STAGED"
kubectl exec -it qserv-ingest-db-0 -- mysql -h localhost -u root -p"$PASSWORD" -e 'select count(*) from qservIngest.chunkfile_queue WHERE locking_pod is not NULL and succeed is NULL;'
echo "LOADED"
kubectl exec -it qserv-ingest-db-0 -- mysql -h localhost -u root -p"$PASSWORD" -e 'select count(*) from qservIngest.chunkfile_queue WHERE succeed is not NULL;'
