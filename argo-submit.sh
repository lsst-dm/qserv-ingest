#!/bin/bash

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)
. $DIR/env.sh

usage() {
  cat << EOD

Usage: `basename $0` [options] path host [host ...]

  Available options:
    -h          this message
    -i          only create tables indexes
    -b          only run query benchmark, take precedence over -i
    -s          allow to run interactively some ingest workflow steps
                take precedence over -i, -b
                use 'kubectl exec -it <podname> -c main -- bash' to open a bash
                in the worklow pod
    -t          Directory for input data parameters (located in $DIR/manifests), override OVERLAY environment variable (defined in env.sh)

  Launch ingest workflow

EOD
}

dest=''
user=''
entrypoint='main'

# get the options
while getopts hibst: c ; do
    case $c in
	    h) usage ; exit 0 ;;
	    i) entrypoint="index-tables" ;;
	    b) entrypoint="benchmark" ;;
	    s) entrypoint="interactive" ;;
      t) OVERLAY="${OPTARG}"  ;;
	    \?) usage ; exit 2 ;;
    esac
done
shift `expr $OPTIND - 1`

if [ $# -ne 0 ] ; then
    usage
    exit 2
fi

CFG_PATH="$DIR/manifests/$OVERLAY/configmap"

if [ ! -d "$CFG_PATH" ]; then
    echo "ERROR Invalid configuration path $CFG_PATH" 1>&2;
    exit 1
fi

kubectl apply -k "$CFG_PATH"
argo submit --serviceaccount=argo-workflow -p image="$INGEST_IMAGE" -p verbose=2 --entrypoint $entrypoint -vvv $DIR/manifests/workflow.yaml
