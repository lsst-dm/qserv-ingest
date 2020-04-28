#!/bin/bash

set -euxo pipefail

WORKER="qserv-worker-0"
PORT="25002"
kubectl exec -it "$WORKER" -c repl-wrk -- sh -c "cd /tmp && \
  curl -lO https://raw.githubusercontent.com/lsst-dm/qserv-DC2/tickets/DM-24587/data/step1_1/chunk_57892.txt && \
  mkdir -p /qserv/data/ingest && \
  qserv-replica-file-ingest FILE localhost $PORT 1 position P /tmp/chunk_57892.txt"
