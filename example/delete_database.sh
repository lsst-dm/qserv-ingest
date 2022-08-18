#!/bin/bash
# Delete a database from Qserv

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

Delete a database from Qserv
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
DB_PASSWORD=""

DB_USER="qsingest"

kubectl exec -it $INSTANCE-ingest-db-0 -- mysql -h localhost -u "$DB_USER" -p"$DB_PASSWORD" -e "DELETE FROM qservIngest.contribfile_queue WHERE \`database\` LIKE '$DATABASE';"
time kubectl exec -it $INSTANCE-repl-ctl-0 -- curl http://localhost:$REPL_CTL_PORT/ingest/database/"$DATABASE"  \
   -X DELETE -H "Content-Type: application/json" \
   -d '{"admin_auth_key":""}'

