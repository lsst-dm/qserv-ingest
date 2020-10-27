Run as non root

## batch ingest

- add parameter
- optimize
  - Use MEMQ+JOB (number of job is workers/ingest-thread), where ingest-thread=ncore
  - https://lsstc.slack.com/archives/C996604NR/p1605234280138600
- check side-effect (when queue for a table is empty)


## CC-IN2P3

- mysql delete error: # TODO manage: MySQLdb._exceptions.OperationalError: (1213, 'Deadlock found when trying to get lock; try restarting transaction')
- dpdd overlap: Sabine

- Bastien: master taint is missing on ccqserv225?

##

- Use FILE-LIST option to load multiple chunks on a worker, sequentially
- "LOAD DATA INFILE" use 20MB/sec per thread
- Is czar db `root` access required for repl ctl or could the replication controller use lower privileges for czar db access?

- 2nd index
https://confluence.lsstcorp.org/pages/viewpage.action?pageId=133333850#UserguidefortheQservIngestsystem(APIversion1)-Buildingthe%22secondaryindex%22

- "auto_build_secondary_index":0, : use parameter instead when registering db.
- Refactor kustomize/manifest dir
