# Enable python3 and mysqlclient
. /stack/loadLSST.bash
setup db
setup python_mysqlclient
setup sqlalchemy

PORT="25080"
REPL_URL="http://qserv-repl-ctl-0.qserv-repl-ctl:${PORT}"
QSERV_INGEST_DIR="/ingest/data"
