#!/bin/sh
# Validate ingest of a database inside Qserv 

# @author  Fabrice Jammes, IN2P3

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. "$DIR"/env.sh

usage() {
  cat << EOD

Usage: `basename $0` [options]

  Available options:
    -h          this message

  Validate an ingest process, running sequentially queries against ingested database

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

servers_opt=''
if [ -e /config-data-url/servers.json ]; then
    servers_opt="-s /config-data-url/servers.json"
fi

replctl-validate --verbose \
    "$DATA_URL" \
    "$QSERV_URL" \
    $servers_opt
