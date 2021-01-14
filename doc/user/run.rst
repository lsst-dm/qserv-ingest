
########################################
Run Qserv ingest on a Kubernetes cluster
########################################

Prerequisites
=============

- An up and running Qserv instance, managed by `qserv-operator <https://qserv-operator.lsst.io>`__ inside a k8s cluster:

.. code:: sh

    $ kubectl get qserv
    NAME    AGE
    qserv   10d

    $ kubectl get pods
    NAME                              READY   STATUS    RESTARTS   AGE
    qserv-czar-0                      3/3     Running   6          10d
    qserv-ingest-db-0                 1/1     Running   2          10d
    qserv-operator-57c75fd7c5-tc4ps   1/1     Running   0          10d
    qserv-repl-ctl-0                  1/1     Running   6          10d
    qserv-repl-db-0                   1/1     Running   2          10d
    qserv-worker-0                    5/5     Running   0          10d
    qserv-worker-1                    5/5     Running   0          10d
    ...
    qserv-worker-7                    5/5     Running   0          10d
    qserv-worker-8                    5/5     Running   0          10d
    qserv-worker-9                    5/5     Running   0          10d
    qserv-xrootd-redirector-0         2/2     Running   0          10d
    qserv-xrootd-redirector-1         2/2     Running   0          10d

- An HTTP server which provide access to all input data and metadata


Prepare Qserv ingest
====================

Get the project
---------------

.. code:: sh

    git clone https://github.com/lsst-dm/qserv-ingest
    cd qserv-ingest

Prepare configuration
---------------------

.. code:: sh

    cp -r manifests/in2p3 manifests/<CUSTOM_INGEST>
    cp env.example.sh env.sh

- In `manifests/<MY_INGEST>/init/kustomization.yaml`, set:
  - `DATA_URL` to the **root URL of the HTTP server serving input data**
  - `REQUESTS_CA_BUNDLE` to the local path of the CA chain of this server, if needed.
- In `env.sh`, set `OVERLAY` to `<CUSTOM_INGEST>`, and eventually `INSTANCE` to the name of current Qserv instance.

Launch Qserv ingest
===================

Register the database to ingest and load the chunk-to-ingest queue:

.. code:: sh

    ./job.sh init

Launch parallel ingest jobs:

.. code:: sh

    ./job.sh ingest


Publish the database:

.. code:: sh

    ./job.sh publish

Create all indexes:

.. code:: sh

    ./job.sh index

Then adapt `example/query.sh` to launch a few queries against freshly ingested data.
