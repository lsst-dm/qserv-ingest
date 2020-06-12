---
apiVersion: v1
kind: ConfigMap
metadata:
  name: repl-creds
data:
  qserv: ''
---
apiVersion: apps/v1
kind: Deployment
metadata:
  name: ingest-dev
  labels:
    app: qserv
    tier: ingest-dev
spec:
  replicas: 1
  selector:
    matchLabels:
     app: qserv
     tier: ingest-dev
  template:
    metadata:
      labels:
        app: qserv
        tier: ingest-dev
    spec:
      containers:
      - command:
        - sleep
        - "3600"
        image: <INGEST_IMAGE>
        imagePullPolicy: Always
        name: ingest
        volumeMounts:
          - name: repl-creds
            mountPath: /qserv/.lsst
      volumes:
        - name: repl-creds
          configMap:
            name: repl-creds
