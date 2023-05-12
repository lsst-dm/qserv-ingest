#!/bin/bash
# Get the number of remaining elements in ingest chunk queue

set -euxo pipefail

DATABASE='skysim5000_v1_1_1_parquet'
DATABASE='dp01_dc2_catalogs'
DATABASE="dp02_dc2_catalogs"
DATABASE='%'

PASSWORD=CHANGEME
kubectl exec -it qserv-ingest-db-0 -- mysql -h localhost -u root -p"$PASSWORD" -e "select * from qservIngest.mutex;"
