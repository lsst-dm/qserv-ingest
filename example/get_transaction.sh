#!/bin/bash
# get a transaction info

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/../env.sh

password=""

usage() {
  cat << EOD

Usage: `basename $0` [options] transaction-id

  Available options:
    -h            this message
    -p password   replication system password, default to ""
    -P port       replication controller port, default to 8080
    
  Retrieve information about a transaction in Qserv replication/ingest system

EOD
}

# get the options
while getopts hp:P: c ; do
    case $c in
	    h) usage ; exit 0 ;;
	    p) password="$OPTARG" ;;
	    P) replication_controller_port="${OPTARG}" ;;
	    \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

if [ $# -ne 1 ] ; then
    usage
    exit 2
fi

transaction_id=$1

# kubectl exec -it qserv-repl-ctl-0 -- curl "http://qserv-repl-ctl:$REPL_CTL_PORT/ingest/trans/$transaction_id" \
#   -X GET  -H "Content-Type: application/json" -d "{\"auth_key\":\"$password\"}"

URL="ingest/trans/$transaction_id?&contrib=1&contrib_long=1"
kubectl exec -it qserv-repl-ctl-0 -- curl "http://qserv-repl-ctl:$REPL_CTL_PORT/$URL" \
  -X GET  -H "Content-Type: application/json" -d "{\"auth_key\":\"$password\"}"

