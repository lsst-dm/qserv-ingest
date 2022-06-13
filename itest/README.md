# Integration test datasets for qserv-ingest

datasets/case01 -> datasets/case05 : Generated from qserv repository, using `get-testdata.sh` on a developmemt workstation



# Notes for preparing a new integration test for qserv-ingest


```shell
./dev_run_ingest.sh
DEV_INGEST_POD=$(kubectl get pods -l app=qserv,org=lsst,tier=ingest-dev -o jsonpath='{.items[0].metadata.name}')
kubectl cp itest/datasets/case01/dbbench.ini "$DEV_INGEST_POD":/tmp/dbbench.ini
kubectl exec -it "$DEV_INGEST_POD" bash
dbbench --url mysql://qsmaster:@qserv-czar:4040 --database qservTest_case01_qserv /tmp/dbbench.ini
exit
kubectl cp default/ingest-dev-75b5c8565d-7qn2b:tmp/dbbench/ /tmp/dbbench-expected
cd /tmp
tar zcvf dbbench-expected.tgz dbbench-expected/
cp /tmp/dbbench-expected.tgz ~/src/qserv-ingest/itest/datasets/case01/
```
