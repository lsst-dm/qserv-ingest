#!/bin/bash
# List contributions in 'IN_PROGRESS' state for a database

set -euo pipefail

usage() {
  cat << EOD

Usage: `basename $0` [options] database

  Available options:
    -h          this message

List contributions in 'IN_PROGRESS' state for a database
EOD
}

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

# Get globals status for in-progress contributions
query="SELECT status,COUNT(*) FROM transaction_contrib WHERE \`database\`='$DATABASE' GROUP BY status;"
kubectl exec -it  qserv-repl-db-0 -c repl-db -- \
    mysql --host=qserv-repl-db-0 --user=qsreplica  qservReplica -e "$query"

# Get detailed status for in-progress contributions
query="SELECT id,transaction_id,worker,\`table\`,chunk,is_overlap,create_time,start_time,read_time,load_time
  FROM transaction_contrib
  WHERE \`database\`='$DATABASE' AND status='IN_PROGRESS';"
kubectl exec -it  qserv-repl-db-0 -c repl-db -- \
    mysql --host=qserv-repl-db-0 --user=qsreplica  qservReplica -e "$query"

