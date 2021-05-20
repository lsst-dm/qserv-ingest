#!/bin/bash

# @author Fabrice Jammes

set -euxo pipefail

ARGO_CLIENT_VERSION="v2.12.11"
echo "Install Argo Workflow client ($ARGO_CLIENT_VERSION)"

curl -sLO https://github.com/argoproj/argo/releases/download/v2.12.11/argo-linux-amd64.gz
gunzip argo-linux-amd64.gz
chmod +x argo-linux-amd64
sudo mv ./argo-linux-amd64 /usr/local/bin/argo
