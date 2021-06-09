#!/bin/bash

# @author: Fabrice Jammes, IN2P3
set -euxo pipefail

echo "Install Argo Workflow inside k8s"
kubectl apply -f https://raw.githubusercontent.com/argoproj/argo/stable/manifests/quick-start-postgres.yaml
kubectl patch configmaps/workflow-controller-configmap -p '{"data":{"containerRuntimeExecutor":"k8sapi"}}'

# Retrieve current namespace
NS=$(kubectl config view --minify --output 'jsonpath={..namespace}')
NS=$([ ! -z "$NS" ] && echo "$NS" || echo "default")

cat <<EOF | kubectl apply -f -
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
  name: argo
  namespace: $NS
roleRef:
  apiGroup: rbac.authorization.k8s.io
  kind: ClusterRole
  name: edit
subjects:
- kind: ServiceAccount
  name: default
  namespace: $NS
EOF

kubectl wait --for=condition=available --timeout=600s deployment argo-server minio postgres workflow-controller
