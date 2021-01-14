- Manage connection parameters
- Test argo-workflow
- Run as non root

## batch ingest

- optimize
  - Use MEMQ+JOB (number of job is workers/ingest-thread), where ingest-thread=ncore
  - https://lsstc.slack.com/archives/C996604NR/p1605234280138600
- check side-effect (when queue for a table is empty)

##

- "LOAD DATA INFILE" use 20MB/sec per thread

- Refactor kustomize/manifest dir
