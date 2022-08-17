#!/bin/bash
# Show processlist on mariadb@worker0
# Useful to monitor Qserv replication service "publish" phase

set -euxo pipefail

usage() {
  cat << EOD

Usage: `basename $0` [options] WORKER_ID

  Available options:
    -h          this message

Show Mariadb process list for a given woker
EOD
}

# get the options
while getopts ht: c ; do
    case $c in
	    h) usage ; exit 0 ;;
	    \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

if [ $# -ne 1 ] ; then
    usage
    exit 2
fi

worker_id=$1

echo "Processlist on mariadb@worker${worker_id}"
kubectl exec -it "qserv-worker-${worker_id}" -c mariadb \
  -- sh -c "mysql -u root -pCHANGEME -h localhost -e 'SHOW PROCESSLIST\G'"
