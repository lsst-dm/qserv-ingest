#!/bin/bash

# Publish a Qserv database, using Qserv replication system

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. $DIR/env.sh

usage() {
  cat << EOD

Usage: `basename $0` [options]
  Available options:
    -h          this message

  Publish a Qserv database, using Qserv replication system

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
replctl -v --config "$INGEST_CONFIG" publish
