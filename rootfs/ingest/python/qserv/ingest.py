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
from multiprocessing.pool import ThreadPool
import os
import posixpath
import time
import urllib.parse

# ----------------------------
# Imports for other modules --
# ----------------------------
from .http import Http
from .metadata import ChunkMetadata
from .queue import QueueManager
from .util import trailing_slash

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

    def index_all_tables(self):
        url = urllib.parse.urljoin(self.repl_url, "replication/sql/index")
        for json_idx in self.chunk_meta.get_json_indexes():
            _LOG.info(f"Create index: {json_idx}")
            self.http.post(url, json_idx)

    def build_secondary_index(self):
        url = urllib.parse.urljoin(self.repl_url, "ingest/index/secondary")
        _LOG.info(f"Create secondary index")
        payload = {"database": self.chunk_meta.database, "allow_for_published":1, "local":1, "rebuild":1}
        self.http.post(url, payload)

    def ingest(self):
        chunk_to_load = True
        while chunk_to_load:
            chunk_to_load = self._ingest_transaction()

    def _ingest_transaction(self):
        """Get a chunk from a queue server,
        then load it inside Qserv,
        during a super-transation

        Returns:
        --------
        Integer number: 0 if no more chunk to load,
                        1 if a chunk was loaded successfully
        """

        _LOG.info("START INGEST TRANSACTION")

        chunks_locked = self.queue_manager.lock_chunks()
        if len(chunks_locked) == 0:
            return False

        transaction_id = None
        chunk = None
        try:
            transaction_id = self._start_transaction()
            ingest_success = self._ingest_parallel_chunks(transaction_id, chunks_locked)
        except Exception as e:
            _LOG.critical('Ingest failed during transaction: %s, %s', transaction_id, e)
            ingest_success = False
            raise(e)
        finally:
            if transaction_id:
                self._close_transaction(transaction_id,
                                        ingest_success)

        # TODO release chunk in queue if process crash
        # so that it can be locked by an other pod?
        # => Doing it manually seems safer.

        # ingest successful
        self.queue_manager.delete_chunks()
        return True

    def _ingest_parallel_chunks(self, transaction_id, chunks_locked):
        with ThreadPool() as pool:
            args = []
            for c in chunks_locked:
                args.append([self.repl_url, self.queue_manager.chunk_meta, transaction_id, c])
            for chunk, startedAt, endedAt in pool.imap_unordered(_ingest_chunk, args):
                logging.debug('Chunk %s ingest started at %s ended at %s' % (chunk, startedAt, endedAt))
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
            tmp_url += "?abort=0"
        else:
            tmp_url += "?abort=1"
        url = urllib.parse.urljoin(self.repl_url, tmp_url)
        responseJson = self.http.put(url)

        for trans in responseJson['databases'][database]['transactions']:
            _LOG.debug("Close transaction (id: %s state: %s)",
                       trans['id'], trans['state'])

def _get_chunk_url(base_url, chunk_id, file_format):
    chunk_filename = file_format.format(chunk_id)
    base_url = trailing_slash(base_url)
    file_url = urllib.parse.urljoin(base_url, chunk_filename)
    _LOG.debug("Chunk file url %s", file_url)
    return file_url


def _get_chunk_location(repl_url, chunk, database, transaction_id):
    url = urllib.parse.urljoin(repl_url, "ingest/chunk")
    payload = {"chunk": chunk,
               "database": database,
               "transaction_id": transaction_id}
    responseJson = Http().post(url, payload)

    # Get location host and port
    host = responseJson["location"]["host"]
    port = responseJson["location"]["port"]
    _LOG.info("Location for chunk %d: %s %d", chunk, host, port)

    return (host, port)

def _ingest_chunk(args):
    repl_url, chunk_meta, transaction_id, chunk = args
    try:
        logging.debug("Start INGEST")
        startedAt = time.strftime("%H:%M:%S", time.localtime())
        (database, chunk_id, chunk_base_url, table) = chunk
        (host, port) = _get_chunk_location(repl_url,
                                           chunk_id,
                                           database,
                                           transaction_id)
        _ingest_file(host, port, transaction_id, chunk_base_url, chunk_id, table, False)
        if chunk_meta.is_director(table):
            _ingest_file(host, port, transaction_id, chunk_base_url, chunk_id, table, True)
        endedAt = time.strftime("%H:%M:%S", time.localtime())
    except Exception as e:
        _LOG.critical('Error in ingest task for chunk %s: %s',
                        chunk,
                        e)
        raise(ValueError(e))
    return chunk, startedAt, endedAt


def _ingest_file(host, port, transaction_id, chunk_base_url, chunk_id, table, overlap):
    # See https://confluence.lsstcorp.org/display/DM/Ingest%3A+11.1.+The+REST+services, section 1.1.7

    if overlap:
        chunk_file_format = "chunk_{}_overlap.txt"
    else:
        chunk_file_format = "chunk_{}.txt"
    chunk_file_url = _get_chunk_url(chunk_base_url, chunk_id, chunk_file_format)
    worker_url = "http://{}:{}".format(host,25004)
    url = urllib.parse.urljoin(worker_url, "ingest/file")
    _LOG.debug("_ingest_chunk: url: %s", url)
    payload = {"transaction_id":transaction_id,
                "table":table,
                "column_separator":",",
                "chunk":chunk_id,
                "overlap":int(overlap),
                "url":chunk_file_url}
    _LOG.debug("_ingest_chunk: payload: %s", payload)
    responseJson = Http().post(url, payload)
    _LOG.debug("Ingest: responseJson: %s", responseJson)

    # TODO manage responseJson error everywhere!!!

