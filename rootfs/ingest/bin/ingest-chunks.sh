#!/bin/bash
# Launch parallel ingest tasks using Qserv replication/ingest service

# @author  Fabrice Jammes, IN2P3

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. $DIR/env.sh

chunk_queue_fraction=''

usage() {
  cat << EOD

Usage: `basename $0` [options] chunk_queue_fraction

  Available options:
    -h          this message

  Launch an ingest process, which will ingest chunk contribution inside a super-transaction,
  stop when no more chunk file remains in chunk queue or if a blocking error occurs
  ingest 'chunk_queue_size/chunk_queue_fraction' chunk files per super-transaction

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

replctl --verbose --config "$INGEST_CONFIG" \
    ingest --chunk-queue-fraction "$chunk_queue_fraction"

