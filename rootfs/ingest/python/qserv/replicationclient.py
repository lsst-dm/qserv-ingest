#!/usr/bin/env python

# LSST Data Management System
# Copyright 2014-2015 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.

"""
Client library for Qserv replication service.

@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from enum import Enum
from functools import lru_cache
import json
import logging
from multiprocessing.pool import ThreadPool
import os
import posixpath
import random
import socket
import time
from typing import Any
import urllib.parse

# ----------------------------
# Imports for other modules --
# ----------------------------
from .http import Http
from . import jsonparser
from .util import increase_wait_time, trailing_slash

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
AUTH_PATH = "~/.lsst/qserv"
_LOG = logging.getLogger(__name__)
_VERSION = 7

# Max attempts to retry ingesting a file on replication service retriable error
MAX_RETRY_ATTEMPTS = 3

class ReplicationClient():
    """
    Python client for the Qserv ingest/replication service
    """

    def __init__(self, repl_url):
        """
        Retrieve chunk metadata and connection to concurrent queue manager
        """

        self.repl_url = trailing_slash(repl_url)

        self.http = Http()

        self._check_version()

        self.index_url = urllib.parse.urljoin(self.repl_url, "replication/sql/index")

    def abort_transactions(self, database):
        """
        Abort all started transactions for a given database
        """
        for transaction_id in self.get_transactions_started(database):
            success = False
            self._close_transaction(transaction_id, success)
            _LOG.info("Abort transaction: %s", transaction_id)

    def build_secondary_index(self, database):
        url = urllib.parse.urljoin(self.repl_url, "ingest/index/secondary")
        _LOG.info("Create secondary index")
        payload = {"database": database, "allow_for_published":1, "local":1, "rebuild":1}
        r = self.http.post(url, payload)
        jsonparser.raise_error(r)

    def close_transaction(self, database, transaction_id, success):
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
        for trans in responseJson['databases'][database]['transactions']:
            _LOG.debug("Close transaction (id: %s state: %s)",
                       trans['id'], trans['state'])

    def _check_version(self):
        url = urllib.parse.urljoin(self.repl_url, "meta/version")
        responseJson = self.http.get(url)
        jsonparser.raise_error(responseJson)
        if responseJson['version'] != _VERSION:
            raise ValueError(f"Invalid replication server version (is {responseJson['version']}, expected {_VERSION})")
        _LOG.info("Replication service version: v%s", _VERSION)

    def database_config(self, database):
        """
        Configure a database inside replication system
        """
        url = urllib.parse.urljoin(self.repl_url, "/ingest/config/")
        json = {"database": database,
                "CAINFO": "/etc/pki/tls/certs/ca-bundle.crt",
                "SSL_VERIFYPEER": 1
            }
        _LOG.debug("Configure database inside replication system, url: %s, json: %s", url, json)
        responseJson = self.http.put(url, json)
        jsonparser.raise_error(responseJson)

    def database_publish(self, database):
        """
        Publish a database inside replication system
        """
        path = "/ingest/database/{}".format(database)
        url = urllib.parse.urljoin(self.repl_url, path)
        _LOG.debug("Publish database: %s", url)
        responseJson = self.http.put(url)
        jsonparser.raise_error(responseJson)

    def database_register(self, json_db):
        """
        Register a database inside replication system
        using data_url/<database_name>.json as input data
        """
        url = urllib.parse.urljoin(self.repl_url, "/ingest/database/")
        payload = json_db
        _LOG.debug("Starting a database registration request: %s with %s", url, payload)
        responseJson = self.http.post(url, payload)
        jsonparser.raise_error(responseJson)

    def database_register_tables(self, tables_json_data, felis):
        """
        Register a database inside replication system
        using data_url/<database_name>.json as input data
        """
        if felis:
            _LOG.info("Loaded Felis schema for tables %s", felis.keys())

        url = urllib.parse.urljoin(self.repl_url, "/ingest/table/")

        if _LOG.isEnabledFor(logging.DEBUG):
            _LOG.debug("Ordered list of table to register")
            for json_data in tables_json_data:
                _LOG.debug(" %s", json_data['table'])

        for json_data in tables_json_data:
            if felis is not None and json_data["table"] in felis:
                schema = felis[json_data["table"]]
                json_data["schema"] = schema + json_data["schema"]
            _LOG.debug("Starting a table registration request: %s with %s",
                        url, json_data)
            responseJson = self.http.post(url, json_data)
            jsonparser.raise_error(responseJson)

    def get_database_status(self, database, family):
        url = urllib.parse.urljoin(self.repl_url, "replication/config")
        responseJson = self.http.get(url)
        jsonparser.raise_error(responseJson)
        status = jsonparser.parse_database_status(responseJson, database, family)
        _LOG.debug(f"Database {family}:{database} status: {status}")
        return status

    def get_current_indexes(self, database, tables: list):
        url = urllib.parse.urljoin(self.repl_url, "replication/sql/index")
        for t in tables:
            params={'database': database,
                    'overlap': 1,
                    'table': t}
            responseJson = self.http.get(self.index_url, params)
            jsonparser.raise_error(responseJson)
            indexes = dict()
            jsonparser.get_indexes(responseJson, indexes)
        _LOG.info("Indexes %s", indexes)
        jsonparser.raise_error(responseJson)
        return indexes

    def _get_transactions(self, states, database):
        """
        Return transactions
        """
        url = urllib.parse.urljoin(self.repl_url,
                                   "ingest/trans?database=" + database)
        responseJson = self.http.get(url)
        jsonparser.raise_error(responseJson)
        transaction_ids = jsonparser.filter_transactions(responseJson, database, states)

        return transaction_ids

    def get_transactions_started(self, database):
        states = [jsonparser.TransactionState.STARTED]
        return self._get_transactions(states, database)

    def get_transactions_not_finished(self, database):
        states = [jsonparser.TransactionState.ABORTED, jsonparser.TransactionState.STARTED]
        return self._get_transactions(states, database)

    def index_all_tables(self, json_indexes: Any):
        for json_idx in json_indexes:
            _LOG.info(f"Create index: {json_idx}")
            responseJson = self.http.post(self.index_url, json_idx)
            jsonparser.raise_error(responseJson)


    @staticmethod
    def ingest_file(host, port, transaction_id, chunk_file_url, chunk_id, table, is_overlap):
        # See https://confluence.lsstcorp.org/display/DM/Ingest%3A+11.1.+The+REST+services, section 1.1.7

        worker_url = "http://{}:{}".format(host,port)
        url = urllib.parse.urljoin(worker_url, "ingest/file")
        _LOG.debug("_ingest_chunk: url: %s", url)
        payload = {"transaction_id":transaction_id,
                    "table":table,
                    "column_separator":",",
                    "chunk":chunk_id,
                    "overlap":int(is_overlap),
                    "url":chunk_file_url}
        _LOG.debug("_ingest_chunk: payload: %s", payload)
        retry_attempts = 0
        retry = True
        responseJson = ""
        wait_sec = 1
        while retry:
            _LOG.debug("_ingest_chunk: url %s, retry attempts: %s, payload: %s", url, retry_attempts, payload)
            responseJson = Http().post(url, payload)
            if retry_attempts < MAX_RETRY_ATTEMPTS:
                check_retry = True
            else:
                check_retry = False
            retry = jsonparser.raise_error(responseJson, check_retry)
            if retry:
                time.sleep(wait_sec)
                wait_sec = increase_wait_time(wait_sec)
                retry_attempts += 1
        _LOG.debug("Ingest: responseJson: %s", responseJson)

    def start_transaction(self, database):
        url = urllib.parse.urljoin(self.repl_url, "ingest/trans")
        payload = {"database": database, "context": {'pod': socket.gethostname()}}
        responseJson = self.http.post(url, payload)
        jsonparser.raise_error(responseJson)

        # For catching the super transaction ID
        # Want to print responseJson["databases"]["hsc_test_w_2020_14_00"]["transactions"]
        current_db = responseJson["databases"][database]
        transaction_id = current_db["transactions"][0]["id"]
        _LOG.debug(f"transaction ID: {transaction_id}")
        return transaction_id

    @lru_cache(maxsize=None)
    def get_chunk_location(self, chunk, database, transaction_id):
        url = urllib.parse.urljoin(self.repl_url, "ingest/chunk")
        payload = {"chunk": chunk,
                "database": database,
                "transaction_id": transaction_id}
        responseJson = Http().post(url, payload)
        jsonparser.raise_error(responseJson)

        host, port = jsonparser.get_location(responseJson)
        _LOG.info("Location for chunk %d: %s %d", chunk, host, port)

        return (host, port)

