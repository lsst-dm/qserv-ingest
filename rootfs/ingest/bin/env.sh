# Enable python3 and mysqlclient
. /stack/loadLSST.bash
setup db
setup python_mysqlclient
setup sqlalchemy

PORT="25080"
BASE_URL="http://qserv-repl-ctl-0.qserv-repl-ctl:${PORT}"
CHUNK_URL="https://raw.githubusercontent.com/lsst-dm/qserv-DC2/tickets/DM-24587/data/step1_1/"
DATABASE="desc_dc2"
QSERV_INGEST_DIR="/ingest/data"

