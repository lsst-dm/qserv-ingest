apiVersion: v1
kind: Pod
metadata:
  name: qserv-ingest
  labels:
    app: qserv
    instance: qserv
    run: qserv-ingest
    tier: ingest
spec:
  containers:
  - command:
    - sleep
    - "3600"
    image: <INGEST_IMAGE>
    imagePullPolicy: Always
    name: qserv-ingest
  
