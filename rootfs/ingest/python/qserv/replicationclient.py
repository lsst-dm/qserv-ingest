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

"""
Client library for Qserv replication service.

@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from functools import lru_cache
import logging
import posixpath
import socket
from typing import Any, Dict, List, Tuple
import urllib.parse

# ----------------------------
# Imports for other modules --
# ----------------------------
from . import jsonparser
from .http import Http, DEFAULT_AUTH_PATH
from . import util

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)
_VERSION = 8


class ReplicationClient:
    """
    Client for the Qserv ingest/replication service

    Use chunk metadata and connection to concurrent queue manager
    """

    def __init__(self, repl_url: str, auth_path: str = DEFAULT_AUTH_PATH) -> None:

        self.repl_url = util.trailing_slash(repl_url)

        self.http = Http()

        self.timeout_short = 5
        self.timeout_long = 120

        self._check_version()

        self.index_url = urllib.parse.urljoin(self.repl_url, "replication/sql/index")

    def abort_transactions(self, database: str) -> None:
        """
        Abort all started transactions for a given database
        """
        for transaction_id in self.get_transactions_started(database):
            success = False
            self.close_transaction(database, transaction_id, success)
            _LOG.info("Abort transaction: %s", transaction_id)

    def build_secondary_index(self, database: str) -> None:
        url = urllib.parse.urljoin(self.repl_url, "ingest/index/secondary")
        _LOG.info("Create secondary index")
        payload = {
            "database": database,
            "allow_for_published": 1,
            "local": 1,
            "rebuild": 1,
        }
        r = self.http.post(url, payload)
        jsonparser.raise_error(r)

    def close_transaction(self, database: str, transaction_id: int, success: bool) -> None:
        """
        Close or abort a transaction
        """
        tmp_url = posixpath.join("ingest/trans/", str(transaction_id))
        if success is True:
            tmp_url += "?abort=0"
        else:
            tmp_url += "?abort=1"
        url = urllib.parse.urljoin(self.repl_url, tmp_url)
        _LOG.debug("Attempt to close transaction (PUT %s)", url)
        responseJson = self.http.put(url)
        jsonparser.raise_error(responseJson)

        # TODO Check if there is only one transaction in responseJson in
        # order to remove 'database' parameter
        for trans in responseJson["databases"][database]["transactions"]:
            _LOG.debug("Close transaction (id: %s state: %s)", trans["id"], trans["state"])

    def _check_version(self) -> None:
        url = urllib.parse.urljoin(self.repl_url, "meta/version")
        responseJson = self.http.get(url, timeout=self.timeout_short)
        jsonparser.raise_error(responseJson)
        if responseJson["version"] != _VERSION:
            raise ValueError(
                "Invalid replication server version " + f"(is {responseJson['version']}, expected {_VERSION})"
            )
        _LOG.info("Replication service version: v%s", _VERSION)

    def database_config(self, database: str, replication_config: util.ReplicationConfig) -> None:
        """Set replication system configuration for a given database
        https://confluence.lsstcorp.org/display/DM/Ingest%3A+11.1.8.1.+Setting+configuration+parameters

        Parameters
        ----------
        database: `str`
            Database name
        replication_config: `util.ReplicationConfig`
            Configuration parameters for the database inside replication/ingest system
        """

        url = urllib.parse.urljoin(self.repl_url, "/ingest/config/")
        json = {
            "database": database,
            "CAINFO": replication_config.cainfo,
            "SSL_VERIFYPEER": replication_config.ssl_verifypeer,
            "LOW_SPEED_LIMIT": replication_config.low_speed_limit,
            "LOW_SPEED_TIME": replication_config.low_speed_time,
        }
        _LOG.debug("Configure database inside replication system, url: %s, json: %s", url, json)
        responseJson = self.http.put(url, json, timeout=self.timeout_long)
        jsonparser.raise_error(responseJson)

    def database_publish(self, database: str) -> None:
        """
        Publish a database inside replication system
        """
        path = "/ingest/database/{}".format(database)
        url = urllib.parse.urljoin(self.repl_url, path)
        _LOG.debug("Publish database: %s", url)
        responseJson = self.http.put(url)
        jsonparser.raise_error(responseJson)

    def database_register(self, json_db: Dict) -> None:
        """
        Register a database inside replication system
        using data_url/<database_name>.json as input data
        """
        url = urllib.parse.urljoin(self.repl_url, "/ingest/database/")
        payload = json_db
        _LOG.debug("Starting a database registration request: %s with %s", url, payload)
        responseJson = self.http.post(url, payload, timeout=self.timeout_long)
        jsonparser.raise_error(responseJson)

    def database_register_tables(self, tables_json_data: List[Dict], felis: Dict = None) -> None:
        """
        Register a database inside replication system
        using data_url/<database_name>.json as input data
        """
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
            responseJson = self.http.post(url, json_data, timeout=self.timeout_long)
            jsonparser.raise_error(responseJson)

    def get_database_status(self, database: str, family: str) -> jsonparser.DatabaseStatus:
        url = urllib.parse.urljoin(self.repl_url, "replication/config")
        responseJson = self.http.get(url, timeout=self.timeout_short)
        jsonparser.raise_error(responseJson)
        status = jsonparser.parse_database_status(responseJson, database, family)
        _LOG.debug(f"Database {family}:{database} status: {status}")
        return status

    @lru_cache(maxsize=None)
    def get_chunk_location(self, chunk_id: int, database: str) -> Tuple[str, int]:
        """Get the location of a chunk for a given database

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
        payload = {"chunk": chunk_id, "database": database}
        responseJson = Http().post(url, payload, timeout=self.timeout_short)
        jsonparser.raise_error(responseJson)

        host, port = jsonparser.get_chunk_location(responseJson)
        _LOG.info("Location for chunk %d: %s:%d", chunk_id, host, port)

        return (host, port)

    @lru_cache(maxsize=None)
    def get_regular_tables_locations(self, database: str) -> List[Tuple[str, int]]:
        """Returns connection parameters of the Data Ingest Service of workers
           which are available for ingesting regular (fully replicated) tables:

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
        responseJson = Http().get(url, payload, timeout=self.timeout_short)
        jsonparser.raise_error(responseJson)

        locations = jsonparser.get_regular_table_locations(responseJson)
        _LOG.info("Locations for regular tables for database %s: %s", database, locations)

        return locations

    def _get_transactions(self, states: List[jsonparser.TransactionState], database: str) -> List[int]:
        """
        Return transactions ids
        """
        url = urllib.parse.urljoin(self.repl_url, "ingest/trans?database=" + database)
        responseJson = self.http.get(url, timeout=self.timeout_short)
        jsonparser.raise_error(responseJson)
        transaction_ids = jsonparser.filter_transactions(responseJson, database, states)

        return transaction_ids

    def get_transactions_started(self, database: str)  -> List[int]:
        states = [jsonparser.TransactionState.STARTED]
        return self._get_transactions(states, database)

    def get_transactions_not_finished(self, database: str) -> List[int]:
        states = [
            jsonparser.TransactionState.ABORTED,
            jsonparser.TransactionState.STARTED,
        ]
        return self._get_transactions(states, database)

    def index_all_tables(self, json_indexes: List[Dict[str, Any]]) -> None:
        for json_idx in json_indexes:
            _LOG.info(f"Create index: {json_idx}")
            responseJson = self.http.post(self.index_url, json_idx)
            jsonparser.raise_error(responseJson)

    def start_transaction(self, database: str) -> int:
        url = urllib.parse.urljoin(self.repl_url, "ingest/trans")
        payload = {"database": database, "context": {"pod": socket.gethostname()}}
        responseJson = self.http.post(url, payload, timeout=self.timeout_short)
        jsonparser.raise_error(responseJson)

        # For catching the super transaction ID
        # Want to print responseJson["databases"]["hsc_test_w_2020_14_00"]["transactions"]
        current_db = responseJson["databases"][database]
        transaction_id = int(current_db["transactions"][0]["id"])
        _LOG.debug("transaction ID: %i", {transaction_id})
        return transaction_id
