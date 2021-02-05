- CHECK before publishing

      def database_publish(self):
        """Publish a database inside replication system
        """
        # Perform some controls because database publication is not reversible
        trans = self.get_transactions_started()
        _LOG.debug(f"Started transactions {trans}")
        if len(trans)>0:
            raise ValueError(f"Database publication prevented by started transactions: {trans}")
        # TODO check if at least one FINISHED transactions?
        # TODO check if chunk queue is empty
        # TODO add jsonparser?

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

