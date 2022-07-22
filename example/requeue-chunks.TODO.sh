#!/bin/bash
# Requeue chunks

set -euxo pipefail

kubectl exec -it qserv-ingest-db-0 -- mysql -h localhost -u root -pCHANGEME -e "update qservIngest.contribfile_queue set locking_pod=NULL WHERE succeed IS NULL"
kubectl exec -it qserv-ingest-db-0 -- mysql -h localhost -u root -pCHANGEME -e 'select * from qservIngest.contribfile_queue'
