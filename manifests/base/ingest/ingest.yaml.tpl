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
            mountPath: /home/qserv/.lsst
          - name: config-data-url
            mountPath: /config-data-url
      restartPolicy: Never
      volumes:
        - name: repl-creds
          secret:
            secretName: repl-creds
        - name: config-data-url
          configMap:
            name: config-data-url
