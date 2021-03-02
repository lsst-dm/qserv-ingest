# Parameters for ingest scripts

data_url="$DATA_URL"
# Strip trailing slash
data_url=$(echo $data_url | sed 's%\(.*[^/]\)/*%\1%')

QUEUE_URL="mysql://qsingest:@qserv-ingest-db-0.qserv-ingest-db/qservIngest"
QSERV_INGEST_DIR="/ingest/data"
REPL_URL="http://qserv-repl-ctl-0.qserv-repl-ctl:25080"

