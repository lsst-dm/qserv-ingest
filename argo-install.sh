#!/bin/bash

set -euxo pipefail

# kubectl apply -f https://raw.githubusercontent.com/argoproj/argo/stable/manifests/quick-start-postgres.yaml
kubectl apply -f manifests/quick-start-postgres.yaml
kubectl patch configmaps/workflow-controller-configmap -p '{"data":{"containerRuntimeExecutor":"k8sapi"}}'

NS=$(kubectl config view --minify --output 'jsonpath={..namespace}')
NS=$([ ! -z "$NS" ] && echo "$NS" || echo "default")

kubectl create rolebinding --clusterrole=edit --serviceaccount="$NS":default argo
