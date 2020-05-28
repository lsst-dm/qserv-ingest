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
  name: qserv-ingest
  labels:
    app: qserv
    instance: qserv
    run: qserv-ingest
    tier: ingest
spec:
  replicas: 1
  selector:
    matchLabels:
     app: qserv
     instance: qserv
     run: qserv-ingest
     tier: ingest
  template:
    metadata:
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
        volumeMounts:
          - name: repl-creds
            mountPath: /qserv/.lsst
      volumes:
        - name: repl-creds
          configMap:
            name: repl-creds
