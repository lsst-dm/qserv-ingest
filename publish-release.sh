#!/usr/bin/env bash

# Publish a qserv-ingest release

# @author  Fabrice Jammes, IN2P3

set -euo pipefail

releasetag=""

DIR=$(cd "$(dirname "$0")"; pwd -P)

set -e

usage() {
  cat << EOD

Usage: `basename $0` [options] RELEASE_TAG

  Available options:
    -h          this message

Create a qserv-ingest release tagged "RELEASE_TAG"
RELEASE_TAG must be of the form YYYY.M.D-rcX
- Push ingest image to docker hub
EOD
}

# get the options
while getopts ht: c ; do
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

releasetag=$1

sed -i "s/^INGEST_RELEASE=.*$/INGEST_RELEASE='$releasetag'/" $DIR/env.example.sh
git add . 
git commit -m "Publish new release $releasetag"
git tag -a "$releasetag" -m "Version $releasetag"
git push --tag
$DIR/build.sh
$DIR/push-image.sh
