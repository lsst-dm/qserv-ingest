## CC-IN2P3

(lsst-scipipe-984c9f7) [qserv@qserv-ingest-558c6d998f-qsb45 /]$ export REQUESTS_CA_BUNDLE=/tmp/CERT.pem
(lsst-scipipe-984c9f7) [qserv@qserv-ingest-558c6d998f-qsb45 /]$ python
Python 3.7.6 (default, Jan  8 2020, 19:59:22)
[GCC 7.3.0] :: Anaconda, Inc. on linux
Type "help", "copyright", "credits" or "license" for more information.
>>> import requests
>>> r = requests.get("https://ccnetlsst01.in2p3.fr:10443/dc2_run2.1i_dr1b/step1_1/chunk_57892.txt")
>>>
(lsst-scipipe-984c9f7) [qserv@qserv-ingest-558c6d998f-qsb45 /]$ echo $DATA_URL

(lsst-scipipe-984c9f7) [qserv@qserv-ingest-558c6d998f-qsb45 /]$ export DATA_URL=https://ccnetlsst01.in2p3.fr:10443/dc2_run2.1i_dr1b/
(lsst-scipipe-984c9f7) [qserv@qserv-ingest-558c6d998f-qsb45 /]$ in
in                infocmp           infotocap         init              install           instance_events
info              infokey           ingest-chunks.sh  insmod            install-info      instmodsh
(lsst-scipipe-984c9f7) [qserv@qserv-ingest-558c6d998f-qsb45 /]$ register.sh
+ replctl-register -v http://qserv-repl-ctl-0.qserv-repl-ctl:25080 https://ccnetlsst01.in2p3.fr:10443/dc2_run2.1i_dr1b/


Add metadata.json back to sps lssttest

##
- Check what happen on repl-task error?

- Replace "True/False" in csv files with "1/0"
- Use FILE-LIST option to load multiple chunks on a worker, sequentially
- Use MEMQ+JOB (number of job is workers/ingest-thread), where ingest-thread=ncore
- "LOAD DATA INFILE" use 20MB/sec per thread
- Is czar db `root` access required for repl ctl or could the replication controller use lower privileges for czar db access?
