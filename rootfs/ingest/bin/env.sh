# Enable python3 and mysqlclient

# Source pathes to qserv eups packages
. /qserv/run/etc/sysconfig/qserv

# Source pathes to ingest packages
export PYTHONPATH="/ingest/python:${PYTHONPATH}"
export PATH="/ingest/bin:${PATH}"

PORT="25080"
REPL_URL="http://qserv-repl-ctl-0.qserv-repl-ctl:${PORT}"
QSERV_INGEST_DIR="/ingest/data"
