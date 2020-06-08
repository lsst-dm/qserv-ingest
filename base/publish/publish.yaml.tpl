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
          value: "https://raw.githubusercontent.com/lsst-dm/qserv-DC2/tickets/DM-24587/data/example_db/"
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
