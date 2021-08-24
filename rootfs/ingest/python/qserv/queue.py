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
Manage a chunk files queue used to orchestrate Qserv replication service
on the client side

@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import logging
import socket
import time

# ----------------------------
# Imports for other modules --
# ----------------------------
from .metadata import ChunkMetadata
import sqlalchemy
from sqlalchemy import MetaData, Table
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import StaticPool
from sqlalchemy.sql import asc, select, delete, func
from .util import increase_wait_time

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

_LOG = logging.getLogger(__name__)


class QueueManager():
    """
    Class implementing chunk queue manager for Qserv ingest process
    """

    def __init__(self, connection, chunk_metadata: ChunkMetadata):

        db_url = make_url(connection)
        self.engine = sqlalchemy.create_engine(db_url, poolclass=StaticPool, pool_pre_ping=True)
        self.pod = socket.gethostname()

        db_meta = MetaData(bind=self.engine)
        self.queue = Table('chunkfile_queue', db_meta, autoload=True)
        self.chunk_meta = chunk_metadata
        self.ordered_tables_to_load = self.chunk_meta.get_tables_names()
        _LOG.debug("Ordered tables to load: %s", self.ordered_tables_to_load)
        self.next_current_table()

    def set_transaction_size(self, chunk_queue_fraction):
        """
        Set number of chunk files managed by a single transaction
        """
        chunk_files_count = self._count_chunk_files_per_database()
        _LOG.debug("Chunk files queue size: %s", chunk_files_count)
        self._chunks_to_lock_number = int(chunk_files_count / chunk_queue_fraction) + 1

    def _count_chunk_files_per_database(self):
        """
        Count chunk files for current database
           if loaded is 'True' count chunk files which are not loaded
           else count all chunks files.
        """
        query = select([func.count('*')]).select_from(self.queue)
        query = query.where(self.queue.c.database == self.chunk_meta.database)
        result = self.engine.execute(query)
        chunk_files_count = next(result)[0]
        result.close()
        return chunk_files_count

    def next_current_table(self):
        if len(self.ordered_tables_to_load) != 0:
            self.current_table = self.ordered_tables_to_load.pop(0)
        else:
            _LOG.warn("No table to load")
            self.current_table = None

    def _get_locked_chunkfiles(self):
        query = select([self.queue.c.database,
                        self.queue.c.chunk_id,
                        self.queue.c.chunk_file_path,
                        self.queue.c.is_overlap,
                        self.queue.c.table])
        query = query.where(self.queue.c.locking_pod == self.pod)
        query = query.where(self.queue.c.succeed.is_(None))
        query = query.where(self.queue.c.database == self.chunk_meta.database)
        result = self.engine.execute(query)
        chunks = result.fetchall()
        result.close()
        return chunks

    def load(self):
        """If queue is empty for current database, then load chunks files in queue
           else do nothing
           Chunks description should be available at chunks_url
        Returns
        -------
        Nothing
        """

        chunk_files_count = self._count_chunk_files_per_database()
        if chunk_files_count != 0:
            _LOG.warn("Chunk queue not empty, skip chunk queue load")
            return

        for (path, chunk_ids, is_overlap, table) in self.chunk_meta.get_chunk_files_info():
            for chunk_id in chunk_ids:
                _LOG.debug("Add chunk (%s, %s, %s) to queue", chunk_id, table, is_overlap)
                self.engine.execute(
                    self.queue.insert(),
                    {"database": self.chunk_meta.database,
                     "chunk_id": chunk_id,
                     "chunk_file_path": path,
                     "is_overlap": is_overlap,
                     "table": table})

    def lock_chunkfiles(self):
        """If some chunk files are already locked in queue for current pod, return them
           if not, lock a batch of them and then return their representation,
           or None if all chunk files have been ingested
        Returns
        -------
        A list of chunk files representation where:
        chunk_file = (string database, int chunk_id, bool is_overlap, string table)
        """

        # Check chunks which were previously locked for this pod
        chunks_locked = self._get_locked_chunkfiles()
        chunks_locked_count = len(chunks_locked)
        if chunks_locked_count != 0:
            _LOG.debug("Chunks already locked by pod: %s",
                       chunks_locked)
        else:
            _LOG.debug("Current table: %s", self.current_table)
            while not self._is_queue_empty() and chunks_locked_count < self._chunks_to_lock_number:

                # SqlAlchemy has only limited support for MariaDB SQL extension
                # See https://docs.sqlalchemy.org/en/14/dialects/mysql.html?highlight=mysql_limit#mysql-mariadb-sql-extensions
                # So code below might be the only remaining solution to use these extensions:
                # sql = "UPDATE chunkfile_queue SET locking_pod = '{}' "
                # sql += "WHERE locking_pod IS NULL AND `table` = '{}' "
                # sql += "AND `database` = '{}' "
                # sql += "ORDER BY chunk_id ASC LIMIT {};"
                # query = sql.format(self.pod, self.current_table,
                #                    self.chunk_meta.database,
                #                    self._chunks_to_lock_number-chunks_locked_count)

                chunk_to_lock = self._chunks_to_lock_number-chunks_locked_count
                query = self.queue.update(mysql_limit=chunk_to_lock).values(locking_pod=self.pod)
                query = query.where(self.queue.c.locking_pod.is_(None))
                query = query.where(self.queue.c.database == self.chunk_meta.database)
                query = query.where(self.queue.c.table == self.current_table)

                _LOG.debug("Query: %s", query)

                self.engine.execute(query)

                chunks_locked = self._get_locked_chunkfiles()
                chunks_locked_count = len(chunks_locked)
                _LOG.debug("chunks_locked_count: %s", chunks_locked_count)
                if chunks_locked_count < self._chunks_to_lock_number:
                    logging.info("No more chunk to lock for table %s",
                                 self.current_table)
                    self.next_current_table()

        logging.debug("Chunks locked by pod %s: %s",
                      self.pod,
                      chunks_locked)
        return chunks_locked

    def release_locked_chunkfiles(self, ingest_success):
        """ Mark chunks files as "succeed" in chunk queue if super-transaction has been successfully commited
            Release chunks files in queue when the super-transaction has been aborted

            WARN: this operation will be retried until it succeed so that chunk queue state is consistent with ingest state
        Returns
        -------
        Integer number, String
        """
        if ingest_success:
            query = self.queue.update().values(succeed=1)
            logging.debug("Mark chunk files as 'succeed' in queue")
            query = self.queue.update().values(succeed=1)
        else:
            logging.debug("Unlock chunk files in queue")
            query = self.queue.update().values(locking_pod=None)

        query = query.where(self.queue.c.locking_pod == self.pod)
        query = query.where(self.queue.c.succeed.is_(None))

        wait_sec = 1
        while True:
            try:
                _LOG.debug("Query: %s", query)
                self.engine.execute(query)
                break
            except:
                _LOG.critical("Retry failed query %s", query)
                time.sleep(wait_sec)
                # Sleep for longer and longer
                wait_sec = increase_wait_time(wait_sec)

    def _is_queue_empty(self):
        return (self.current_table == None)

    def get_noningested_chunkfiles(self):
        """Return all chunk files in queue not successfully loaded for current database
        """
        query = select([self.queue.c.database,
                        self.queue.c.chunk_id,
                        self.queue.c.chunk_file_path,
                        self.queue.c.is_overlap,
                        self.queue.c.table])
        query = query.where(self.queue.c.succeed.is_(None))
        query = query.where(self.queue.c.database == self.chunk_meta.database)
        result = self.engine.execute(query)
        chunks = result.fetchall()
        return chunks
