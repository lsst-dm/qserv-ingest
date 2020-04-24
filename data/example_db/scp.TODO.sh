#!/bin/sh

set -e

scp -r cc:/sps/lssttest/qserv/dataloader/dc2_run2_1i_dr1b_complet/step1_1 . 
scp -r cc:/sps/lssttest/qserv/dataloader/dc2_run2_1i_dr1b_complet/step1_2 . 
scp -r cc:/sps/lssttest/qserv/dataloader/dc2_run2_1i_dr1b_complet/step2_2 .
scp -r cc:/sps/lssttest/qserv/dataloader/dc2_run2_1i_dr1b_complet/step2_1 .
scp -r cc:/sps/lssttest/qserv/dataloader/dc2_run2_1i_dr1b_complet/dc2_run2_1i_dr1b_complet.json .
scp -r cc:/sps/lssttest/qserv/dataloader/dc2_run2_1i_dr1b_complet/dpdd_forced.json .
scp -r cc:/sps/lssttest/qserv/dataloader/dc2_run2_1i_dr1b_complet/dpdd_ref.json .
scp -r cc:/sps/lssttest/qserv/dataloader/dc2_run2_1i_dr1b_complet/position.json .
scp -r cc:/sps/lssttest/qserv/dataloader/dc2_run2_1i_dr1b_complet/metadata.json .
