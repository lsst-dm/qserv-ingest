#!/bin/bash
# Set Qserv service as a NodePort on port 30040

set -euxo pipefail

kubectl patch svc qserv-qserv --type='json' \
    -p '[{"op":"replace","path":"/spec/type","value":"NodePort"},{"op":"replace","path":"/spec/ports/0/nodePort","value":30040}]'
