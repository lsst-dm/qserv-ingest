#!/bin/bash
# Extract chunk data sample from in2p3

set -euxo pipefail

SERVER=cc
DATADIR=/sps/lssttest/qserv/dataloader/dc2_run2_1i_dr1b

scp -r $SERVER:$DATADIR/step1_1 . 
scp -r $SERVER:$DATADIR/step1_2 . 
scp -r $SERVER:$DATADIR/step2_2 .
scp -r $SERVER:$DATADIR/step2_1 .
scp -r $SERVER:$DATADIR/dc2_run2_1i_dr1b_complet.json .
scp -r $SERVER:$DATADIR/dpdd_forced.json .
scp -r $SERVER:$DATADIR/dpdd_ref.json .
scp -r $SERVER:$DATADIR/position.json .
scp -r $SERVER:$DATADIR/metadata.json .
