## CC-IN2P3

- mysql delete error: # TODO manage: MySQLdb._exceptions.OperationalError: (1213, 'Deadlock found when trying to get lock; try restarting transaction')
- dpdd overlap: Sabine

- Bastien: master taint is missing on ccqserv225?

##
- Check what happen on repl-task error?

- Use FILE-LIST option to load multiple chunks on a worker, sequentially
- Use MEMQ+JOB (number of job is workers/ingest-thread), where ingest-thread=ncore
- "LOAD DATA INFILE" use 20MB/sec per thread
- Is czar db `root` access required for repl ctl or could the replication controller use lower privileges for czar db access?

- 2nd index
https://confluence.lsstcorp.org/pages/viewpage.action?pageId=133333850#UserguidefortheQservIngestsystem(APIversion1)-Buildingthe%22secondaryindex%22
