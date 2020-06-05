# Enable python3 and mysqlclient
. /stack/loadLSST.bash
setup db
setup python_mysqlclient
setup sqlalchemy

PORT="25080"
REPL_URL="http://qserv-repl-ctl-0.qserv-repl-ctl:${PORT}"
DATA_URL="https://raw.githubusercontent.com/lsst-dm/qserv-DC2/tickets/DM-24587/data/example_db"
QSERV_INGEST_DIR="/ingest/data"

