## Validate ticket/DM-29567 on kind/CI

- put sql+result on github release
- Improve error management for job see https://stackoverflow.com/questions/69266116/do-not-delete-pods-even-if-job-fails and argoWorkflow 'withItems' parameter

## Argo/Config

See if example/config.yaml can be set in argo CLI (i.e. in params.yaml):
https://argoproj.github.io/argo-workflows/examples/#parameters
argo submit arguments-parameters.yaml --parameter-file params.yaml


## In validate, SQLAlchemy has a weird bahavior:

  Query: SELECT count(*) AS count_1
  FROM dpdd_ref
  Query total time: 0.069575
  Query result: 0

  Result with regular mysql client is 37??



## two-mode:
  * crash on error, as before
  * continue at max: cancel ingest for chunks which produce some special error or have been ingested too much time without success.


- Improve error recovery: if transaction fails, then check chunk queue state (non terminated tasks) before relaunching workflow and ask for chunk queue manuel cleanup

- Improve management of connection parameters for input data
- Improve argo-workflow install
- Run as non root

## batch ingest

- optimize
  - Use MEMQ+JOB (number of job is workers/ingest-thread), where ingest-thread=ncore
  - https://lsstc.slack.com/archives/C996604NR/p1605234280138600
- "LOAD DATA INFILE" use 20MB/sec per thread

