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
        env:
        - name: DATA_URL
          valueFrom:
            configMapKeyRef:
              name: config-data-url
              key: DATA_URL
        - name: "REQUESTS_CA_BUNDLE"
          valueFrom:
            configMapKeyRef:
              name: config-data-url
              key: REQUESTS_CA_BUNDLE
        volumeMounts:
        - name: config-data-url
          mountPath: /config-data-url
      volumes:
        - name: config-data-url
          configMap:
            name: config-data-url
        image: <INGEST_IMAGE>
        imagePullPolicy: Always
        name: ingest
        volumeMounts:
          - name: repl-creds
            mountPath: /home/qserv/.lsst
      volumes:
        - name: repl-creds
          secret:
            name: secret-repl-creds-qserv
