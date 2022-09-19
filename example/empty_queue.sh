#!/bin/bash
# Remove ingest queue entries for a given database 

# Database names:
# - cosmoDC2_v1_1_4_image_overlap
# - dc2_run2_1i_dr1b
# - dc2_errors
# - dc2_object_run22i_dr6_wfd_v2_00

# Default password
# PASSWORD=CHANGEME

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
