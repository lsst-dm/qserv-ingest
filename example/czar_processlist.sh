#!/bin/bash
# Show processlist on mariadb@czar

set -euxo pipefail

echo "Processlist on mariadb@czar"
kubectl exec -it qserv-repl-ctl-0 -c repl-ctl \
  -- mysql -u root -pCHANGEME -h qserv-czar qservMeta -e 'SHOW PROCESSLIST\G'
