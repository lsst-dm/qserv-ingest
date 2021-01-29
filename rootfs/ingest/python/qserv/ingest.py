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
from enum import Enum
from functools import lru_cache
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
from . import jsonparser
from .metadata import ChunkMetadata
from .queue import QueueManager
from .util import trailing_slash

TMP_DIR = "/tmp"

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
AUTH_PATH = "~/.lsst/qserv"
_LOG = logging.getLogger(__name__)

class IngestArgs():
    def __init__(self, host, port, transaction_id, chunk_file_url, chunk_id, table, is_overlap):
        """ Store input parameters for replication REST api located at http://<host:port>/ingest/file
        """
        self.host = host
        self.port = port
        self.transaction_id = transaction_id
        self.chunk_file_url = chunk_file_url
        self.chunk_id = chunk_id
        self.table = table
        self.is_overlap = is_overlap

    def get_kwargs(self):
        return self.__dict__

class Ingester():
    """Manage chunk ingestion tasks
    """

    def __init__(self, data_url, repl_url, queue_url=None, servers_file=None):
        """ Retrieve chunk metadata and connection to concurrent queue manager
        """

        self.repl_url = trailing_slash(repl_url)

        self.http = Http()

        self.chunk_meta = ChunkMetadata(data_url, servers_file)
        if queue_url is not None:
            self.queue_manager = QueueManager(queue_url, self.chunk_meta)


    def abort_transactions(self):
        for transaction_id in self.get_transactions_started():
            success = False
            self._close_transaction(transaction_id, success)
            _LOG.info("Abort transaction: %s", transaction_id)

    def build_secondary_index(self):
        url = urllib.parse.urljoin(self.repl_url, "ingest/index/secondary")
        _LOG.info(f"Create secondary index")
        payload = {"database": self.chunk_meta.database, "allow_for_published":1, "local":1, "rebuild":1}
        self.http.post(url, payload)

    def check_supertransactions_success(self):
        """ Check all super-transactions have ran successfully
        """
        trans = self.get_transactions_started()
        _LOG.debug(f"IDs of transactions in STARTED state: {trans}")
        if len(trans)>0:
            raise Exception(f"Database publication prevented by started transactions: {trans}")
        chunks = self.queue_manager.get_noningested_chunkfiles()
        if len(chunks)>0:
            _LOG.error(f"Non ingested chunk files: {chunks}")
            raise Exception(f"Database publication forbidden: non-ingested chunk files: {len(chunks)}")
        _LOG.info("All chunk files in queue successfully ingested")

    def _compute_args(self, chunks_locked, transaction_id):
        args = []
        for i, chunk_file in enumerate(chunks_locked):
            (database, chunk_id, chunk_file_path, is_overlap, table) = chunk_file
            if is_overlap:
                chunk_file_format = "chunk_{}_overlap.txt"
            else:
                chunk_file_format = "chunk_{}.txt"

            base_url = self.queue_manager.chunk_meta.get_loadbalancer_url(i)
            chunk_file_url = self._get_chunk_file_url(base_url, chunk_file_path, chunk_id, chunk_file_format)
            (host, port) = _get_chunk_location(self.repl_url,
                                               chunk_id,
                                               database,
                                               transaction_id)

            ingest_args = IngestArgs(host, port, transaction_id, chunk_file_url, chunk_id, table, is_overlap)
            args.append(ingest_args)
        return args

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

    def database_publish(self):
        """Publish a database inside replication system
        """
        path = "/ingest/database/{}".format(self.chunk_meta.database)
        url = urllib.parse.urljoin(self.repl_url, path)
        _LOG.debug("Publish database: %s", url)
        self.http.put(url)

    def database_register(self):
        """Register a database inside replication system
           using data_url/<database_name>.json as input data
        """
        url = urllib.parse.urljoin(self.repl_url, "/ingest/database/")
        payload = self.chunk_meta.json_db
        _LOG.debug("Starting a database registration request: %s with %s", url, payload)
        self.http.post(url, payload)

    def database_register_tables(self, felis=None):
        """Register a database inside replication system
           using data_url/<database_name>.json as input data
        """
        if felis:
            _LOG.info("Loaded Felis schema for tables %s", felis.keys())

        url = urllib.parse.urljoin(self.repl_url, "/ingest/table/")

        for json_data in self.chunk_meta.get_tables_json():
            if felis is not None and json_data["table"] in felis:
                schema = felis[json_data["table"]]
                json_data["schema"] = schema + json_data["schema"]
            _LOG.debug("Starting a table registration request: %s with %s",
                        url, json_data)
            self.http.post(url, json_data)

    def database_config(self):
        """Configure a database inside replication system
        """
        url = urllib.parse.urljoin(self.repl_url, "/ingest/config/")
        json = {"database": self.chunk_meta.database,
                "CAINFO": "/etc/pki/tls/certs/ca-bundle.crt",
                "SSL_VERIFYPEER": 1
            }
        _LOG.debug("Configure database inside replication system, url: %s, json: %s", url, json)
        self.http.put(url, json)

    def _get_chunk_file_url(self, base_url, chunk_file_path, chunk_id, file_format):
        chunk_filename = file_format.format(chunk_id)
        relative_url = os.path.join(chunk_file_path, chunk_filename)
        file_url = urllib.parse.urljoin(base_url, relative_url)
        _LOG.debug("Chunk file url %s", file_url)
        return file_url

    def get_db_status(self):
        url = urllib.parse.urljoin(self.repl_url, "replication/config")
        responseJson = self.http.get(url)
        status = jsonparser.parse_database_status(responseJson, self.chunk_meta.database, self.chunk_meta.family)
        _LOG.debug(f"Database {self.chunk_meta.family}:{self.chunk_meta.database} status: {status}")
        return status

    def _get_transactions(self, states):
        """
        Return transactions
        """
        database = self.chunk_meta.database
        url = urllib.parse.urljoin(self.repl_url,
                                   "ingest/trans?database=" + database)
        responseJson = self.http.get(url)
        transaction_ids = jsonparser.filter_transactions(responseJson, database, states)

        return transaction_ids

    def get_transactions_started(self):
        states = [jsonparser.TransactionState.STARTED]
        return self._get_transactions(states)

    def get_transactions_not_finished(self):
        states = [jsonparser.TransactionState.ABORTED, jsonparser.TransactionState.STARTED]
        return self._get_transactions(states)

    def index_all_tables(self):
        url = urllib.parse.urljoin(self.repl_url, "replication/sql/index")
        for json_idx in self.chunk_meta.get_json_indexes():
            _LOG.info(f"Create index: {json_idx}")
            self.http.post(url, json_idx)

    def ingest(self, chunk_queue_fraction):
        self.queue_manager.set_transaction_size(chunk_queue_fraction)
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

        chunks_locked = self.queue_manager.lock_chunkfiles()
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
        self.queue_manager.unlock_chunks()
        return True

    def _ingest_parallel_chunks(self, transaction_id, chunks_locked):
        args = self._compute_args(chunks_locked, transaction_id)
        with ThreadPool() as pool:
            for chunk, startedAt, endedAt in pool.imap_unordered(_ingest_chunk, args):
                logging.debug('Chunk %s ingest started at %s ended at %s' % (chunk, startedAt, endedAt))
        return True

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

@lru_cache(maxsize=None)
def _get_chunk_location(repl_url, chunk, database, transaction_id):
    url = urllib.parse.urljoin(repl_url, "ingest/chunk")
    payload = {"chunk": chunk,
               "database": database,
               "transaction_id": transaction_id}
    responseJson = Http().post(url, payload)

    host, port = jsonparser.get_location(responseJson)
    _LOG.info("Location for chunk %d: %s %d", chunk, host, port)

    return (host, port)

def _ingest_chunk(ingest_args):

    kwargs = ingest_args.get_kwargs()
    chunk_file_url = kwargs['chunk_file_url']
    try:
        logging.debug("Start ingesting chunk contribution: %s", chunk_file_url)
        startedAt = time.strftime("%H:%M:%S", time.localtime())

        _ingest_file(**kwargs)
        endedAt = time.strftime("%H:%M:%S", time.localtime())
    except Exception as e:
        _LOG.critical('Error while ingesting chunk contribution: %s: %s',
                      chunk_file_url,
                      e)
        raise(ValueError(e))
    return chunk_file_url, startedAt, endedAt


def _ingest_file(host, port, transaction_id, chunk_file_url, chunk_id, table, is_overlap):
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
    responseJson = Http().post(url, payload)
    _LOG.debug("Ingest: responseJson: %s", responseJson)
