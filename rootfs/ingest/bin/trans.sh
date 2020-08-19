#!/bin/bash

# Manage not completed transactions

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh

set -euxo pipefail

usage() {
  cat << EOD

Usage: `basename $0` [options]

  Available options:
    -h          this message
    -C          delete all pending transactions for a database
    -v          verbose

  List and eventually delete all pending ingest transactions for a given database

EOD
}

DELETE_OPT=""
VERBOSE_OPT=""

# get the options
while getopts hCv c ; do
    case $c in
	    h) usage ; exit 0 ;;
	    C) DELETE_OPT="--cleanup" ;;
	    v) VERBOSE_OPT="--verbose" ;;
	    \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

if [ $# -gt 1 ] ; then
    usage
    exit 2
fi

replctl-trans $VERBOSE_OPT $DELETE_OPT "$REPL_URL" "$DATA_URL"
