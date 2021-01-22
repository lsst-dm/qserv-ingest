#!/bin/bash
# Get the number of remaining elements in ingest chunk queue

set -euxo pipefail


PASSWORD=CHANGEME
echo "NOT STAGED"
kubectl exec -it qserv-ingest-db-0 -- mysql -h localhost -u root -p"$PASSWORD" -e 'select count(*) from qservIngest.task WHERE pod is NULL;'
echo "STAGED"
kubectl exec -it qserv-ingest-db-0 -- mysql -h localhost -u root -p"$PASSWORD" -e 'select count(*) from qservIngest.task WHERE pod is not NULL and succeed is NULL;'
echo "LOADED"
kubectl exec -it qserv-ingest-db-0 -- mysql -h localhost -u root -p"$PASSWORD" -e 'select count(*) from qservIngest.task WHERE succeed is not NULL;'
