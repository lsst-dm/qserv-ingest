#!/bin/bash
# Get the number of remaining elements in ingest chunk queue

set -euxo pipefail

DATABASE='skysim5000_v1_1_1_parquet'
DATABASE='dp01_dc2_catalogs'
DATABASE="dp02_dc2_catalogs"
DATABASE='%'

PASSWORD=CHANGEME
echo "NOT STAGED"
kubectl exec -it qserv-ingest-db-0 -- mysql -h localhost -u root -p"$PASSWORD" -e "select count(*) from qservIngest.contribfile_queue WHERE locking_pod is NULL and \`database\` LIKE '$DATABASE';"
echo "STAGED"
kubectl exec -it qserv-ingest-db-0 -- mysql -h localhost -u root -p"$PASSWORD" -e "select count(*) from qservIngest.contribfile_queue WHERE locking_pod is not NULL and succeed is NULL and \`database\` LIKE '$DATABASE';"
echo "LOADED"
kubectl exec -it qserv-ingest-db-0 -- mysql -h localhost -u root -p"$PASSWORD" -e "select count(*) from qservIngest.contribfile_queue WHERE succeed is not NULL and \`database\` LIKE '$DATABASE';"
