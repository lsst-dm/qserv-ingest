apiVersion: batch/v1
kind: Job
metadata:
  name: ingest-publish
spec:
  template:
    spec:
      containers:
      - name: publish 
        command:
        - publish.sh
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
