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
from enum import Enum, auto
from functools import lru_cache
import json
import logging
from multiprocessing.pool import ThreadPool
import os
import posixpath
import random
import socket
import time
from typing import List
import urllib.parse

# ----------------------------
# Imports for other modules --
# ----------------------------
from .http import Http
from . import jsonparser
from .metadata import ChunkMetadata
from .queue import QueueManager
from .util import increase_wait_time, trailing_slash
from .replicationclient import ReplicationClient

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
AUTH_PATH = "~/.lsst/qserv"
_LOG = logging.getLogger(__name__)

# Max attempts to retry ingesting a file on replication service retriable error
MAX_RETRY_ATTEMPTS = 3

class TransactionAction(Enum):
    ABORT_ALL = auto()
    CLOSE = auto()
    CLOSE_ALL = auto()
    LIST_STARTED = auto()
    START = auto()

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
    """
    Manage chunk ingestion tasks
    """

    def __init__(self, chunk_metadata: ChunkMetadata, replication_url: str, queue_manager: QueueManager=None):
        """
        Retrieve chunk metadata and connection to concurrent queue manager
        """
        self.chunk_meta = chunk_metadata
        self.queue_manager = queue_manager
        self.http = Http()
        self.repl_client = ReplicationClient(replication_url)


    def check_supertransactions_success(self):
        """ Check all super-transactions have ran successfully
        """
        trans = self.repl_client.get_transactions_started(self.chunk_meta.database)
        _LOG.debug(f"IDs of transactions in STARTED state: {trans}")
        if len(trans)>0:
            raise Exception(f"Database publication prevented by started transactions: {trans}")
        chunks = self.queue_manager.get_noningested_chunkfiles()
        if len(chunks)>0:
            _LOG.error(f"Non ingested chunk files: {chunks}")
            raise Exception(f"Database publication forbidden: non-ingested chunk files: {len(chunks)}")
        _LOG.info("All chunk files in queue successfully ingested")

    def _compute_args(self, chunks_locked, transaction_id: int):
        args = []
        for i, chunk_file in enumerate(chunks_locked):
            (database, chunk_id, chunk_file_path, is_overlap, table) = chunk_file
            if is_overlap:
                chunk_file_format = "chunk_{}_overlap.txt"
            else:
                chunk_file_format = "chunk_{}.txt"

            base_url = self.queue_manager.chunk_meta.get_loadbalancer_url(i)
            chunk_file_url = self._get_chunk_file_url(base_url, chunk_file_path, chunk_id, chunk_file_format)
            (host, port) = self.repl_client.get_chunk_location(chunk_id,
                                                               database,
                                                               transaction_id)

            ingest_args = IngestArgs(host, port, transaction_id, chunk_file_url, chunk_id, table, is_overlap)
            args.append(ingest_args)
        return args

    def database_publish(self):
        """
        Publish a Qserv database inside replication system
        """
        database = self.chunk_meta.database
        db_status = self.repl_client.database_publish(database)

    def database_register_and_config(self, felis=None):
        """
        Register a database, its tables and its configuration inside replication system
        using data_url/<database_name>.json as input data
        """
        self.repl_client.database_register(self.chunk_meta.json_db)
        self.repl_client.database_register_tables(self.chunk_meta.get_tables_json(), felis)
        self.repl_client.database_config(self.chunk_meta.database)


    def _get_chunk_file_url(self, base_url, chunk_file_path, chunk_id, file_format):
        chunk_filename = file_format.format(chunk_id)
        relative_url = os.path.join(chunk_file_path, chunk_filename)
        file_url = urllib.parse.urljoin(base_url, relative_url)
        _LOG.debug("Chunk file url %s", file_url)
        return file_url

    def get_database_status(self):
        """
        Return the status of a Qserv catalog database
        """
        return self.repl_client.get_database_status(self.chunk_meta.database, self.chunk_meta.family)

    def ingest(self, chunk_queue_fraction):
        """
        Ingest chunk files for a transaction
        """
        self.queue_manager.set_transaction_size(chunk_queue_fraction)
        chunk_to_load = True
        while chunk_to_load:
            chunk_to_load = self._ingest_transaction()

    def index(self, secondary=False):
        """
        Index Qserv shared tables or create secondary index
        """
        if secondary:
            database = self.chunk_meta.database
            self.repl_client.build_secondary_index(database)
        else:
            json_indexes = self.chunk_meta.get_json_indexes()
            self.repl_client.index_all_tables(json_indexes)

    def _ingest_transaction(self):
        """
        Get a chunk from a queue server,
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
            transaction_id = self.repl_client.start_transaction(self.chunk_meta.database)
            ingest_success = self._ingest_parallel_contributions(transaction_id, chunks_locked)
        except Exception as e:
            _LOG.critical('Ingest failed during transaction: %s, %s', transaction_id, e)
            ingest_success = False
            # Stop process when any transaction abort
            raise(e)
        finally:
            if transaction_id:
                self.repl_client.close_transaction(self.chunk_meta.database,
                                       transaction_id,
                                       ingest_success)
                self.queue_manager.release_locked_chunkfiles(ingest_success)

        return True

    def _ingest_parallel_contributions(self, transaction_id: int, chunks_locked):
        args = self._compute_args(chunks_locked, transaction_id)
        with ThreadPool() as pool:
            for chunk, startedAt, endedAt in pool.imap_unordered(_ingest_contribution, args):
                logging.debug('Chunk %s ingest started at %s ended at %s' % (chunk, startedAt, endedAt))
        return True

    def transaction_helper(self, action: TransactionAction, trans_id: List[int]=None):
        """
        High-level method which help in managing transaction(s)
        """
        database = self.chunk_meta.database
        if TransactionAction.ABORT_ALL:
            self.repl_client.abort_transactions(database)
        elif TransactionAction.START:
            transaction_id = self.repl_client.start_transaction(database)
            _LOG.info("Start transaction %s", transaction_id)
        elif TransactionAction.CLOSE:
            self.repl_client.close_transaction(database, trans_id, True)
            _LOG.info("Commit transaction %s", trans_id)
        elif TransactionAction.CLOSE_ALL:
            transaction_ids = self.repl_client.get_transactions_started(database)
            for i in transaction_ids:
                self.repl_client.close_transaction(database, i, True)
                _LOG.info("Commit transaction %s", i)
        elif TransactionAction.LIST_STARTED:
            self.repl_client.get_transactions_started(database)

def _ingest_contribution(ingest_args):

    kwargs = ingest_args.get_kwargs()
    chunk_file_url = kwargs['chunk_file_url']
    try:
        _LOG.debug("Start ingesting chunk contribution: %s", chunk_file_url)
        startedAt = time.strftime("%H:%M:%S", time.localtime())

        ReplicationClient.ingest_file(**kwargs)
        endedAt = time.strftime("%H:%M:%S", time.localtime())
        _LOG.debug("Finished ingesting chunk contribution: %s", chunk_file_url)
    except Exception as e:
        _LOG.critical('Error while ingesting chunk contribution: %s: %s',
                      chunk_file_url,
                      e)
        raise(e)
    return chunk_file_url, startedAt, endedAt



