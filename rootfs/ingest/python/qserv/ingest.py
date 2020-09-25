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
User-friendly client library for Qserv replication service.

@author  Hsin Fang Chiang, Rubin Observatory
@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import logging
import os
import posixpath
import subprocess
import urllib.parse

# ----------------------------
# Imports for other modules --
# ----------------------------
from .http import Http
from .metadata import ChunkMetadata
from .queue import QueueManager
from .util import download_file, trailing_slash

TMP_DIR = "/tmp"

ABORTED_STATE = "ABORTED"
FINISHED_STATE = "FINISHED"

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
AUTH_PATH = "~/.lsst/qserv"
_LOG = logging.getLogger(__name__)


class Ingester():
    """Manage chunk ingestion tasks
    """

    def __init__(self, data_url, repl_url, queue_url=None):
        """ Retrieve chunk metadata and connection to concurrent queue manager
        """

        self.repl_url = trailing_slash(repl_url)

        self.http = Http()

        self.chunk_meta = ChunkMetadata(data_url)
        if queue_url is not None:
            self.queue_manager = QueueManager(queue_url, self.chunk_meta)

    def _get_chunk_location(self, chunk, database, transaction_id):
        url = urllib.parse.urljoin(self.repl_url, "ingest/chunk")
        payload = {"chunk": chunk,
                   "database": database,
                   "transaction_id": transaction_id}
        responseJson = self.http.post(url, payload)

        # Get location host and port
        host = responseJson["location"]["host"]
        port = responseJson["location"]["port"]
        _LOG.info("Location for chunk %d: %s %d", chunk, host, port)

        return (host, port)

    def index_all(self):
        url = urllib.parse.urljoin(self.repl_url, "replication/sql/index")
        for json_idx in self.chunk_meta.get_json_indexes():
            _LOG.info(f"Create index: {json_idx}")
            self.http.post(url, json_idx)

    def ingest(self):
        chunk_to_load = True
        while chunk_to_load:
            chunk_to_load = self.ingest_task()

    def ingest_task(self):
        """Get a chunk from a queue server,
        load it inside Qserv,
        during a super-transation

        Returns:
        --------
        Integer number: 0 if no chunk to load,
                        1 if chunk was loaded successfully
        """

        _LOG.info("START INGEST TASK")

        chunk_info = self.queue_manager.lock_chunk()
        if not chunk_info:
            return False

        (database, chunk_id, chunk_base_url, table) = chunk_info
        chunk_file = None
        transaction_id = None
        try:
            transaction_id = self._start_transaction()
            (host, port) = self._get_chunk_location(chunk_id,
                                                    database,
                                                    transaction_id)
            chunk_file = _download_chunk(chunk_base_url, chunk_id,
                                         "chunk_{}.txt")
            _ingest_chunk(host, port, transaction_id, chunk_file, table)
            if self.queue_manager.chunk_meta.is_director(table):
                chunk_file = _download_chunk(
                    chunk_base_url, chunk_id, "chunk_{}_overlap.txt")
                _ingest_chunk(host, port, transaction_id, chunk_file, table)
            ingest_success = True
        except Exception as e:
            _LOG.critical('Error in ingest task for chunk %s: %s',
                          chunk_info,
                          e)
            ingest_success = False
            raise(e)
        finally:
            if transaction_id:
                self._close_transaction(database, transaction_id,
                                        ingest_success)

        # TODO release chunk in queue if process crash
        # so that it can be locked by an other pod?
        # => Doing it manually seems safer.

        # ingest successful
        self.queue_manager.delete_chunk()
        if chunk_file and os.path.isfile(chunk_file):
            os.remove(chunk_file)
        return True

    def get_transactions(self):
        database = self.chunk_meta.database
        url = urllib.parse.urljoin(self.repl_url,
                                   "ingest/trans?database=" + database)
        responseJson = self.http.get(url)

        current_db = responseJson["databases"][database]
        transaction_id = current_db["transactions"][0]["id"]
        _LOG.debug(f"transaction ID: {transaction_id}")

        transaction_ids = []
        transactions = responseJson['databases'][database]['transactions']
        if len(transactions) != 0:
            _LOG.info("Transactions in flight:")
            for trans in transactions:
                _LOG.info("  id: %s state: %s",
                          trans['id'], trans['state'])
                if trans['state'] not in [ABORTED_STATE, FINISHED_STATE]:
                    transaction_ids.append(trans['id'])

        return transaction_ids

    def abort_transactions(self):
        for transaction_id in self.get_transactions():
            success = False
            self._close_transaction(transaction_id, success)
            _LOG.info("Abort transaction: %s", transaction_id)

    def _start_transaction(self):
        database = self.chunk_meta.database
        url = urllib.parse.urljoin(self.repl_url, "ingest/trans")
        payload = {"database": database}
        responseJson = self.http.post(url, payload)

        # For catching the super transaction ID
        # Want to print responseJson["databases"]["hsc_test_w_2020_14_00"]["transactions"]
        current_db = responseJson["databases"][database]
        transaction_id = current_db["transactions"][0]["id"]
        _LOG.debug(f"transaction ID: {transaction_id}")
        return transaction_id

    def _close_transaction(self, transaction_id, success):
        database = self.chunk_meta.database
        tmp_url = posixpath.join("ingest/trans/", str(transaction_id))
        if success is True:
            tmp_url += "?abort=0&build-secondary-index=1"
        else:
            tmp_url += "?abort=1"
        url = urllib.parse.urljoin(self.repl_url, tmp_url)
        responseJson = self.http.put(url)

        for trans in responseJson['databases'][database]['transactions']:
            _LOG.debug("Close transaction (id: %s state: %s)",
                       trans['id'], trans['state'])


def _ingest_chunk(host, port, transaction_id, chunk_file, table):

    cmd = ['qserv-replica-file-ingest', '--debug', '--verbose', 'FILE',
           host, str(port), str(transaction_id), table, "P", chunk_file]
    _LOG.debug("Launch unix process: %s", cmd)

    try:
        result = subprocess.run(cmd,
                                capture_output=True,
                                universal_newlines=True,
                                check=True)
    except subprocess.CalledProcessError as e:
        _LOG.error("stdout %s", e.stdout)
        _LOG.error("stderr %s", e.stderr)
        raise(e)

    _LOG.debug("stdout: '%s'", result.stdout)
    _LOG.debug("stderr: '%s'", result.stderr)


def _download_chunk(base_url, chunk_id, file_format):
    chunk_filename = file_format.format(chunk_id)
    abs_filename = download_file(base_url, chunk_filename)
    return abs_filename



