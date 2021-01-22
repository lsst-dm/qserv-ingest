#!/bin/bash
# Requeue chunks

set -euxo pipefail

kubectl exec -it qserv-ingest-db-0 -- mysql -h localhost -u root -pCHANGEME -e "update qservIngest.task set pod=NULL, start=NULL WHERE succeed IS NULL"
kubectl exec -it qserv-ingest-db-0 -- mysql -h localhost -u root -pCHANGEME -e 'select * from qservIngest.task'
