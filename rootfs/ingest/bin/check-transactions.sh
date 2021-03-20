#!/bin/sh
# Check if all ingest super-transactions have ran successfully.

# @author  Fabrice Jammes, IN2P3

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh

usage() {
  cat << EOD

Usage: `basename $0`

  Available options:
    -h          this message

  Check if all ingest super-transactions have ran successfully.
  Use \$DATA_URL to access input chunk files.

EOD
}

# get the options
while getopts d:h c ; do
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

# strip trailing slash

replctl-ingest --check \
    --verbose \
    "$DATA_URL" \
    "$QUEUE_URL" \
    "$REPL_URL"
