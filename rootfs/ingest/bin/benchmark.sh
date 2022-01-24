#!/bin/bash
# Run a set of queries against an ingested dataset

# @author  Fabrice Jammes, IN2P3

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. $DIR/env.sh

usage() {
  cat << EOD

Usage: `basename $0` [options]

  Available options:
    -h          this message

  Run a set of queries against an ingested dataset and check their results

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
replctl --verbose --config "$INGEST_CONFIG" benchmark