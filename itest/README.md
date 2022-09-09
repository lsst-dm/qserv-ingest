# Integration test datasets for qserv-ingest

Directories `datasets/case01` to `datasets/case05` are manually generated using qserv repository integration test datasets and script `get-testdata.sh` on a developememt workstation.

# Notes for preparing a new integration test for qserv-ingest

```bash
# Launch and open an interactive shell in qserv-ingest development pod
cd ~/src/qserv-ingest
./dev_local.sh

# Switch to workstation, in an other terminal
CASE_ID="case03"
DEV_INGEST_POD=$(kubectl get pods -l app=qserv,org=lsst,tier=ingest-dev -o jsonpath='{.items[0].metadata.name}')
docker cp itest/datasets/"$CASE_ID"/dbbench.ini qserv-ingest:/tmp/dbbench.ini

# Switch to qserv-ingest development pod
CASE_ID="case03"
mkdir /tmp/dbench
dbbench --url mysql://qsmaster:@qserv-czar:4040 --database qservTest_"$CASE_ID"_qserv /tmp/dbbench.ini

# Switch to workstation
docker cp qserv-ingest:tmp/dbbench/ /tmp/dbbench-expected
tar zcvf ./itest/datasets/"$CASE_ID"/dbbench-expected.tgz /tmp/dbbench-expected/
```
