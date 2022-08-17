#!/bin/bash
# List contributions in 'READ_FAILED' state for a database

set -euo pipefail

usage() {
  cat << EOD

Usage: `basename $0` [options] database

  Available options:
    -h          this message

List 15 contributions in 'READ_FAILED' state for a database
EOD
}

kind=false

# get the options
while getopts h c ; do
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

DATABASE="$1"
LIMIT="LIMIT 15"

# Get globals status for in-progress contributions
query="SELECT id, url, transaction_id,worker,http_error,system_error,error \
  FROM transaction_contrib \
  WHERE \`database\`='$DATABASE' AND status='READ_FAILED' $LIMIT;"
kubectl exec -it  qserv-repl-db-0 -c repl-db -- \
    mysql --host=qserv-repl-db-0 --user=qsreplica  qservReplica -e "$query"