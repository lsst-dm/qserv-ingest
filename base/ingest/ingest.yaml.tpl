apiVersion: batch/v1
kind: Job
metadata:
  name: ingest-ingest
spec:
  parallelism: 4
  template:
    spec:
      containers:
      - name: ingest 
        command:
        - ingest-chunks.sh
        env:
        - name: DATA_URL
          valueFrom:
            configMapKeyRef:
              name: config-data-url
              key: DATA_URL
        image: <INGEST_IMAGE> 
        imagePullPolicy: Always
        volumeMounts:
          - name: repl-creds
            mountPath: /qserv/.lsst
      restartPolicy: OnFailure
      volumes:
        - name: repl-creds
          secret:
            secretName: repl-creds
