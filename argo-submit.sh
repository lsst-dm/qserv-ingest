#!/bin/bash

set -euxo pipefail

usage() {
  cat << EOD

Usage: `basename $0` [options] path host [host ...]

  Available options:
    -h          this message
    -i          only create tables indexes
    -b          only run query benchmark (override -i)

  Launch ingest workflow

EOD
}

dest=''
user=''
entrypoint='main'

# get the options
while getopts hib c ; do
    case $c in
	    h) usage ; exit 0 ;;
	    i) entrypoint="index-tables" ;;
	    b) entrypoint="benchmark" ;;
	    \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

DIR=$(cd "$(dirname "$0")"; pwd -P)
. $DIR/env.sh

kubectl apply -k $DIR/manifests/$OVERLAY/configmap
argo submit --serviceaccount=argo-workflow -p image="$INGEST_IMAGE" --entrypoint $entrypoint -vvv $DIR/manifests/workflow.yaml
