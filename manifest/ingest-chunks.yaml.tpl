---
apiVersion: v1
kind: ConfigMap
metadata:
  name: repl-creds
data:
  qserv: ''
---
apiVersion: batch/v1
kind: Job
metadata:
  name: ingest-chunks
  labels:
    app: qserv
    instance: qserv
    tier: ingest-chunks
spec:
  parallelism: 4
  template:
    spec:
      containers:
      - command:
        - ingest-chunks.sh 
        image: <INGEST_IMAGE>
        imagePullPolicy: Always
        name: qserv-ingest
        volumeMounts:
          - name: repl-creds
            mountPath: /qserv/.lsst
      restartPolicy: OnFailure
      volumes:
        - name: repl-creds
          configMap:
            name: repl-creds
