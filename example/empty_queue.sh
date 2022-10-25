#!/bin/bash
# Remove ingest queue entries for a given database

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/../env.sh

usage() {
  cat << EOD

Usage: `basename $0` [options] database

  Available options:
    -h          this message

Empty queue entries for a given database in Qserv-ingest
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

DB_USER="qsingest"

POD="$INSTANCE-ingest-db-0"
kubectl exec -it "$POD" -- mysql -h "$POD" -u "$DB_USER" -e "DELETE FROM qservIngest.contribfile_queue WHERE \`database\` LIKE '$DATABASE';"
