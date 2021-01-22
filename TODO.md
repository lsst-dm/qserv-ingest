- Remove hard-coded port 25004
- Ask for nice startup of replication system (i.e. wait for db)
- Improve error recovery: if transaction fails, then check chunk queue state (non terminated tasks) before relaunching workflow and ask for chunk queue manuel cleanup

- Improve management of connection parameters for input data
- Improve argo-workflow install
- Run as non root

## batch ingest

- optimize
  - Use MEMQ+JOB (number of job is workers/ingest-thread), where ingest-thread=ncore
  - https://lsstc.slack.com/archives/C996604NR/p1605234280138600
- "LOAD DATA INFILE" use 20MB/sec per thread

