#!/bin/sh
# Launch parallel ingest tasks using Qserv replication/ingest service

# @author  Fabrice Jammes, IN2P3

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh

chunk_queue_fraction=''

usage() {
  cat << EOD

Usage: `basename $0` [options] chunk_queue_fraction

  Available options:
    -h          this message

  Launch an ingest process, running sequentially super-transactions,
  stop when no more chunk file remains in chunk queue,
  ingest chunk_queue_size/chunk_queue_fraction chunk files per super-transaction
  Use \$DATA_URL to access input data.

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

if [ "$1" -neq "$1" ] 2>/dev/null ; then
    >&2 echo "ERROR: parameter must be an integer."
    usage
    exit 3
fi

chunk_queue_fraction=$1

servers_opt=''
if [ -e /config-data-url/servers.json ]; then
    servers_opt="-s /config-data-url/servers.json"
fi

replctl-ingest --verbose \
    --chunk_queue-fraction "$chunk_queue_fraction" \
    "$data_url" \
    "$QUEUE_URL" \
    "$REPL_URL" \
    $servers_opt
