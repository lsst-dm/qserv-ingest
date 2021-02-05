#!/bin/sh

# Ask Qserv replication system to publish a Qserv database

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh

usage() {
  cat << EOD

Usage: `basename $0` [options]
  Available options:
    -h          this message

  Launch an ingest process, running sequentially super-transactions,
  stop when no more chunk file remains in chunk queue,
  ingest chunk_queue_size/chunk_queue_fraction chunk files per super-transaction.
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

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

# Publish database
replctl-publish -v "$data_url" \
    "$REPL_URL"

