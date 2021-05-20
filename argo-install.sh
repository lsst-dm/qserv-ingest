#!/bin/bash

# @author: Fabrice Jammes, IN2P3
set -euxo pipefail

echo "Install Argo Workflow inside k8s"
kubectl apply -f https://raw.githubusercontent.com/argoproj/argo/stable/manifests/quick-start-postgres.yaml
kubectl patch configmaps/workflow-controller-configmap -p '{"data":{"containerRuntimeExecutor":"k8sapi"}}'

# Retrieve current namespace
NS=$(kubectl config view --minify --output 'jsonpath={..namespace}')
NS=$([ ! -z "$NS" ] && echo "$NS" || echo "default")

kubectl create rolebinding --clusterrole=edit --serviceaccount="$NS":default argo

kubectl wait --for=condition=available --timeout=600s deployment argo-server minio postgres workflow-controller
