
#############
Prerequisites
#############

- An up and running Qserv instance managed by `qserv-operator <https://qserv-operator.lsst.io>`__ inside a Kubernetes cluster:

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

The namespace containing this instance will be called `<QSERV_NAMESPACE>`.

- Prvileges to create pods and persistent volumes inside `<QSERV_NAMESPACE>`.

- An HTTP(s) server providing access to input data and metadata. All pods inside `<QSERV_NAMESPACE>` must be able to access this HTTP server.

- An instance of Argo Workflow running in `<QSERV_NAMESPACE>`. It can be installed with the script `argo-install.sh` located at the top-level of the `qserv-ingest ` directory.