INGEST_POD=$(kubectl get pods -l tier=ingest -o jsonpath="{.items[0].metadata.name}")

INSTANCE="qserv"
