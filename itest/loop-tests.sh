#!/bin/bash

# LSST Data Management System
# Copyright 2014 LSST Corporation.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.
# Launch Qserv multinode tests on Swarm cluster

# @author Fabrice Jammes SLAC/IN2P3

set -euxo pipefail

DIR=$(cd "$(dirname "$0")"; pwd -P)

INGEST_DIR=$(readlink -f $DIR/..)

INSTANCE=$(kubectl get qservs.qserv.lsst.org -o=jsonpath='{.items[0].metadata.name}')

echo "::notice Run integration tests for Qserv"
$DIR/start-dataserver.sh

# Use qserv-ingest development version
ENV_FILE="$INGEST_DIR"/env.sh
cp "$INGEST_DIR"/env.example.sh "$ENV_FILE"
sed -i -e "s/^INGEST_RELEASE=.*$/INGEST_RELEASE=''/" "$ENV_FILE"

argo delete --all && kubectl delete job -l app=qserv,tier=ingest
stern -l "app=qserv" > /tmp/ingestloop.log &
while true; do

  # Cleanup
  $DIR/../example/delete_database.sh "qcase01"
  $DIR/../example/delete_database.sh "qcase03"
  $DIR/../example/delete_database.sh "dc2_run2_1i_dr1b"

  TEST_CASES="base case01 case03"
  for test_case in $TEST_CASES; do

    echo "---------------------------------------------------------"
    echo "::notice Launch workflow for test case: $test_case"
    echo "---------------------------------------------------------"

    "$INGEST_DIR"/argo-submit.sh -t "$test_case"
    argo watch @latest
    PODS_ARGO_FAILED=$(kubectl get pods -l workflows.argoproj.io/completed=true -o jsonpath='{.items[*].metadata.name}' --field-selector=status.phase=Failed)
    for pod in $PODS_ARGO_FAILED
    do
      echo "Pod $pod log (test case: $test_case):"
      echo "-----------------------------------------"
      kubectl logs $pod -c main
      echo "-----------------------------------------"
    done
    TRANSACTION_JOB=$(kubectl get job -l app=qserv,tier=ingest --output=jsonpath="{.items[0].metadata.name}")
    kubectl get pod -l app=qserv,tier=ingest,job-name="$TRANSACTION_JOB"
    TRANSACTION_FAILED_PODS=$(kubectl get pod -l app=qserv,tier=ingest,job-name=$TRANSACTION_JOB --field-selector='status.phase!=Succeeded,status.phase!=Running' -o custom-columns=POD:metadata.name --no-headers)
    for pod in $TRANSACTION_FAILED_PODS
    do
      echo "::warning Failed transaction pod $pod log:"
      echo "-----------------------------------------"
      kubectl logs $pod
      echo "-----------------------------------------"
    done
    WF_PHASE=$(kubectl get workflow -o jsonpath="{.items[0].status.phase}")
    if [ "$WF_PHASE" == "Failed" ]
    then
      >&2 echo "---------------------------------------------------------"
      >&2 echo "::error fail workflow for test case: $test_case"
      >&2 echo "---------------------------------------------------------"
      exit 1
    else
      echo "---------------------------------------------------------"
      echo "::notice Pass successfully test case: $test_case"
      echo "---------------------------------------------------------"
    fi
    # FIXME get current workflow name instead of keeping only one workflow
    argo delete @latest
  done
done
