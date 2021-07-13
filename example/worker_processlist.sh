#!/bin/bash
# Show processlist on mariadb@worker0
# Useful to monitor Qserv replication service "publish" phase

set -euxo pipefail

echo "Processlist on mariadb@worker0"
kubectl exec -it qserv-worker-0 -c mariadb \
  -- sh -c ". /qserv/stack/loadLSST.bash && setup mariadb && mysql -u root -pCHANGEME -h localhost -e 'SHOW PROCESSLIST\G'"
