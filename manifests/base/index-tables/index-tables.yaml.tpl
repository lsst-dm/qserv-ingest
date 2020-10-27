apiVersion: batch/v1
kind: Job
metadata:
  name: ingest-index-tables
spec:
  template:
    spec:
      containers:
      - name: index 
        command:
        - index-tables.sh
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
      restartPolicy: Never
      volumes:
        - name: repl-creds
          secret:
            secretName: repl-creds
