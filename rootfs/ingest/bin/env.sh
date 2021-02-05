# Parameters for ingest scripts

data_url="$DATA_URL"
# Strip trailing slash
data_url=$(echo $data_url | sed 's%\(.*[^/]\)/*%\1%')

PORT="25080"
QUEUE_URL="mysql://qsingest:@qserv-ingest-db-0.qserv-ingest-db:3306/qservIngest"
QSERV_INGEST_DIR="/ingest/data"
REPL_URL="http://qserv-repl-ctl-0.qserv-repl-ctl:${PORT}"

