#!/bin/bash

# Install Argo workflow pods and client

set -euxo pipefail

# Install argo workflow
kubectl apply -f https://raw.githubusercontent.com/argoproj/argo/stable/manifests/quick-start-postgres.yaml
kubectl patch configmaps/workflow-controller-configmap -p '{"data":{"containerRuntimeExecutor":"k8sapi"}}'

NS=$(kubectl config view --minify --output 'jsonpath={..namespace}')
NS=$([ ! -z "$NS" ] && echo "$NS" || echo "default")

kubectl create rolebinding --clusterrole=edit --serviceaccount="$NS":default argo

# Install argo client
ARGO_CLIENT_VERSION="v2.12.11"
# Download the binary
curl -sLO https://github.com/argoproj/argo/releases/download/v2.12.11/argo-linux-amd64.gz
# Unzip
gunzip argo-linux-amd64.gz
# Make binary executable
chmod +x argo-linux-amd64
# Move binary to path
sudo mv ./argo-linux-amd64 /usr/local/bin/argo

kubectl wait --for=condition=available --timeout=600s deployment argo-server minio postgres workflow-controller