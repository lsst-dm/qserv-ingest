#!/bin/bash
# get a transaction info

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/../env.sh

password=""

usage() {
  cat << EOD

Usage: `basename $0` [options] database

  Available options:
    -h            this message
    -p password   replication system password, default to ""

  Retrieve replication/ingest system configuration

EOD
}

# get the options
while getopts hp: c ; do
    case $c in
	    h) usage ; exit 0 ;;
	    p) password="$OPTARG" ;;
	    \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

if [ $# -ne 1 ] ; then
    usage
    exit 2
fi

database="$1"
password=""

url="ingest/config?database=$database"
kubectl exec -it qserv-repl-ctl-0 -- curl "http://qserv-repl-ctl:$REPL_CTL_PORT/$url" \
  -X GET  -H "Content-Type: application/json" -d "{\"auth_key\":\"$password\"}"
