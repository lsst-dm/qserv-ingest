# This file is part of qserv.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""Client library for Qserv replication service.

@author  Fabrice Jammes, IN2P3

"""

import logging
import posixpath
import socket
import sys
import urllib.parse

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from functools import lru_cache
from typing import Any, Dict, List, Tuple

# ----------------------------
# Imports for other modules --
# ----------------------------
from . import jsonparser, util, version
from .exception import IngestError
from .http import DEFAULT_AUTH_PATH, Http, get_fqdn
from .ingestconfig import IngestServiceConfig

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------


_LOG = logging.getLogger(__name__)


class ReplicationClient:
    """Client for the Qserv ingest/replication service.
    Use chunk metadata and connection to concurrent queue manager
    """

    def __init__(
        self, repl_url: str, timeout_read_sec: int, timeout_write_sec: int, auth_path: str = DEFAULT_AUTH_PATH
    ) -> None:

        self.repl_url = util.trailing_slash(repl_url)
        self.http = Http(timeout_read_sec, timeout_write_sec, auth_path)
        self._report_version()
        self.index_url = urllib.parse.urljoin(self.repl_url, "replication/sql/index")

    def abort_all_transactions(self, database: str) -> None:
        """Abort all started transactions for a given database."""
        for transaction_id in self.get_transactions_inprogress(database):
            success = False
            self.close_transaction(database, transaction_id, success)
            _LOG.info("Abort transaction: %s", transaction_id)

    def build_secondary_index(self, database: str) -> None:
        url = urllib.parse.urljoin(self.repl_url, "ingest/index/secondary")
        _LOG.info("Create secondary index")
        payload = {
            "version": version.REPL_SERVICE_VERSION,
            "database": database,
            "allow_for_published": 1,
            "local": 1,
            "rebuild": 1,
        }
        self.http.post(url, payload)

    def close_transaction(self, database: str, transaction_id: int, success: bool) -> None:
        """Close or abort a transaction."""
        tmp_url = posixpath.join("ingest/trans/", str(transaction_id))
        if success is True:
            tmp_url += "?abort=0"
        else:
            tmp_url += "?abort=1"
        url = urllib.parse.urljoin(self.repl_url, tmp_url)
        _LOG.debug("Attempt to close transaction (PUT %s)", url)
        responseJson = self.http.put(url, payload=None, no_readtimeout=True)

        # TODO Check if there is only one transaction in responseJson in
        # order to remove 'database' parameter
        for trans in responseJson["databases"][database]["transactions"]:
            _LOG.debug("Close transaction (id: %s state: %s)", trans["id"], trans["state"])

    def _report_version(self) -> None:
        """Get and report replication service version"""
        url = urllib.parse.urljoin(self.repl_url, "meta/version")
        responseJson = self.http.get(url)
        _LOG.info("Replication service version: %s, this application's version: %s", responseJson["version"], version.REPL_SERVICE_VERSION)

    def database_config(self, database: str, ingest_service_config: IngestServiceConfig) -> None:
        """Set replication system configuration for a given database https://co
        nfluence.lsstcorp.org/display/DM/Ingest%3A+11.1.8.1.+Setting+configurat
        ion+parameters.

        Parameters
        ----------
        database: `str`
            Database name
        replication_config: `util.IngestServiceConfig`
            Configuration parameters for the database inside
            replication/ingest system

        """
        json = {
            "version": version.REPL_SERVICE_VERSION,
            "database": database,
            "CAINFO": ingest_service_config.cainfo,
            "SSL_VERIFYPEER": ingest_service_config.ssl_verifypeer,
        }

        if ingest_service_config.async_proc_limit is not None:
            json["ASYNC_PROC_LIMIT"] = ingest_service_config.async_proc_limit
        if ingest_service_config.async_proc_limit is not None:
            json["LOW_SPEED_LIMIT"] = ingest_service_config.low_speed_limit
        if ingest_service_config.async_proc_limit is not None:
            json["LOW_SPEED_TIME"] = ingest_service_config.low_speed_time

        url = urllib.parse.urljoin(self.repl_url, "/ingest/config/")
        _LOG.debug("Configure database inside replication system, url: %s, json: %s", url, json)
        self.http.put(url, json)

    def database_publish(self, database: str) -> None:
        """Publish a database inside replication system."""
        path = "/ingest/database/{}".format(database)
        url = urllib.parse.urljoin(self.repl_url, path)
        _LOG.debug("Publish database: %s", url)
        self.http.put(url, no_readtimeout=True)

    def database_register(self, json_db: Dict) -> None:
        """Register a database inside replication system using
        data_url/<database_name>.json as input data."""
        url = urllib.parse.urljoin(self.repl_url, "/ingest/database/")
        payload = json_db
        _LOG.debug("Starting a database registration request: %s with %s", url, payload)
        self.http.post_retry(url, payload)

    def database_register_tables(self, tables_json_data: List[Dict], felis: Dict = None) -> None:
        """Register a database inside replication system using
        data_url/<database_name>.json as input data."""
        if felis is not None:
            _LOG.info("Load Felis schema for tables %s", felis.keys())

        url = urllib.parse.urljoin(self.repl_url, "/ingest/table/")

        if _LOG.isEnabledFor(logging.DEBUG):
            _LOG.debug("Ordered list of table to register")
            for json_data in tables_json_data:
                _LOG.debug(" %s", json_data["table"])

        for json_data in tables_json_data:
            if felis is not None and json_data["table"] in felis:
                schema = felis[json_data["table"]]
                json_data["schema"] = schema + json_data["schema"]
            _LOG.debug("Start a table registration request: %s with %s", url, json_data)
            self.http.post_retry(url, json_data)

    def get_database_status(self, database: str, family: str) -> jsonparser.DatabaseStatus:
        url = urllib.parse.urljoin(self.repl_url, "replication/config")
        responseJson = self.http.get(url)
        status = jsonparser.parse_database_status(responseJson, database, family)
        _LOG.debug(f"Database {family}:{database} status: {status}")
        return status

    # FIXME this might use a lot of memory
    @lru_cache(maxsize=128)
    def get_chunk_location(self, chunk_id: int, database: str) -> Tuple[str, int]:
        """Get the location of a chunk for a given database.

        Parameters
        ----------
        chunk : `str`
            Chunk id.
        database : `str`
            Database name.

        Returns
        -------
        x : `str`
            Hostname of the qserv worker which store the chunk
        y : `int`
            Port number of the of replication service on
            the qserv worker which store the chunk

        """
        url = urllib.parse.urljoin(self.repl_url, "ingest/chunk")
        payload = {
            "version": version.REPL_SERVICE_VERSION,
            "chunk": chunk_id,
            "database": database,
        }
        responseJson = Http().post_retry(url, payload)

        fqdns, port = jsonparser.get_chunk_location(responseJson)
        host = get_fqdn(fqdns, port)
        if not host:
            raise IngestError(f"Unable to find a valid worker fqdn in json response {responseJson}")
        _LOG.info("Location for chunk %d: %s:%d", chunk_id, host, port)

        return (host, port)

    @lru_cache(maxsize=1)
    def get_regular_tables_locations(self, database: str) -> List[Tuple[str, int]]:
        """Returns connection parameters of the Data Ingest Service of workers
        which are available for ingesting regular (fully replicated) tables.

        Parameters
        ----------
        database : `str`
            Database name.

        Returns
        -------
        x : `str`
            Hostname of the qserv worker which store the chunk
        y : `int`
            Port number of the of replication service on
            the qserv worker which store the chunk

        """
        url = urllib.parse.urljoin(self.repl_url, "ingest/regular")
        payload = {"database": database}
        responseJson = Http().get(url, payload)

        sanitized_locations: List[Tuple[str, int]] = []
        locations = jsonparser.get_regular_table_locations(responseJson)
        for (fqdns, port) in locations:
            fqdn = get_fqdn(fqdns, port)
            if not fqdn:
                raise IngestError(f"Unable to find a valid worker fqdn in json response {responseJson}")
            sanitized_locations.append((fqdn, port))

        _LOG.info("Locations for regular tables for database %s: %s", database, locations)

        return sanitized_locations

    def _get_transactions(self, states: List[jsonparser.TransactionState], database: str) -> List[int]:
        """Return transactions ids."""
        url = urllib.parse.urljoin(self.repl_url, "ingest/trans?database=" + database)
        responseJson = self.http.get(url)
        transaction_ids = jsonparser.filter_transactions(responseJson, database, states)

        return transaction_ids

    def get_transactions_inprogress(self, database: str) -> List[int]:
        """Get transaction in progress (i.e. not in FINISHED, ABORTED state)
        for a given database

        Parameters
        ----------
        database : str
            target database

        Returns
        -------
        List[int]
            List of transactions
        """

        states = [
            jsonparser.TransactionState.ABORT_FAILED,
            jsonparser.TransactionState.FINISH_FAILED,
            jsonparser.TransactionState.IS_ABORTING,
            jsonparser.TransactionState.IS_FINISHING,
            jsonparser.TransactionState.IS_STARTING,
            jsonparser.TransactionState.STARTED,
            jsonparser.TransactionState.START_FAILED,
        ]
        trans = self._get_transactions(states, database)
        _LOG.debug(f"IDs of transactions not in FINISHED, ABORTED state: {trans} for {database} database.")
        return trans

    def index_all_tables(self, json_indexes: List[Dict[str, Any]]) -> None:
        for json_idx in json_indexes:
            _LOG.info(f"Create index: {json_idx}")
            self.http.post(self.index_url, json_idx)

    def start_transaction(self, database: str) -> int:
        url = urllib.parse.urljoin(self.repl_url, "ingest/trans")
        payload = {
            "version": version.REPL_SERVICE_VERSION,
            "database": database,
            "context": {"pod": socket.gethostname()},
        }
        responseJson = self.http.post_retry(url, payload)

        # For catching the super transaction ID
        # Want to print
        # responseJson["databases"][<database_name>]["transactions"]
        current_db = responseJson["databases"][database]
        transaction_id = int(current_db["transactions"][0]["id"])
        _LOG.debug("transaction ID: %i", transaction_id)
        return transaction_id

    def deploy_statistics(self, database: str, table_names: List[str]) -> None:
        """Collect row counters in the specified tables and deploy the
        statistics in Qserv to allow optimizations of the relevant queries.
        See:
        - https://confluence.lsstcorp.org/display/DM/3.+Managing+statistics+for+the+row+counters+optimizations # noqa: W505, E501
        - https://confluence.lsstcorp.org/display/DM/1.+Collecting+row+counters+and+deploying+them+at+Qserv

        Parameters
        ----------
        database : `str`
            Database name.
        table_names: `str`
            Names of processed tables

        Raises
        ------
        ReplicationControllerError
             Raised in case of error in JSON response
             for a non-retriable request
        """

        url = urllib.parse.urljoin(self.repl_url, "/ingest/table-stats/")

        # TODO Check parameters with Igor
        payload = {
            "database": database,
            "overlap_selector": "CHUNK_AND_OVERLAP",
            "force_rescan": 1,
            "row_counters_state_update_policy": "ENABLED",
            "row_counters_deploy_at_qserv": 1,
        }

        for table in table_names:
            _LOG.debug("Start a table statistics deployment request: %s with %s", url, table)
            payload["table"] = table
            self.http.post_retry(url, payload, auth=True, no_readtimeout=True)
