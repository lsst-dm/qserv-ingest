#!/bin/bash
# Extract chunk data sample from in2p3

set -euxo pipefail
DIR=$(cd "$(dirname "$0")"; pwd -P)
SERVER=cc

DB="cosmoDC2_v1_1_4_image_testintegration"
SRC_DIR="/sps/lssttest/qserv/dataloader/$DB"
DEST_DIR="$DIR"

scp -r $SERVER:"$SRC_DIR" "$DEST_DIR" 
