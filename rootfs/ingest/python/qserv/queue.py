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

@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import logging
import socket

# ----------------------------
# Imports for other modules --
# ----------------------------
import sqlalchemy
from sqlalchemy import MetaData, Table
from sqlalchemy.engine.url import make_url
from sqlalchemy.sql import select, delete, func

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

_LOG = logging.getLogger(__name__)


class QueueManager():
    """Class implementing chunk queue manager for Qserv ingest process
    """

    def __init__(self, connection, chunk_meta):

        db_url = make_url(connection)
        self.engine = sqlalchemy.create_engine(db_url)
        self.pod = socket.gethostname()

        db_meta = MetaData(bind=self.engine)
        self.task = Table('task', db_meta, autoload=True)
        self.chunk_meta = chunk_meta
        self.ordered_tables_to_load = self.chunk_meta.get_tables_names()
        _LOG.debug("Ordered tables to load: %s", self.ordered_tables_to_load)
        self.next_current_table()
        result = self.engine.execute(select([func.count('*')]).select_from(self.task))
        self.chunk_files_count = next(result)[0]

        _LOG.debug("Number of chunk file to load: %s", self.chunk_files_count)

        # TODO add a parameter
        nb_transaction = 10
        tmp = int(self.chunk_files_count / nb_transaction)
        if tmp != 0:
            self._chunks_to_lock_number = tmp
        else:
            self._chunks_to_lock_number = 1

    def next_current_table(self):
        if len(self.ordered_tables_to_load) != 0:
            self.current_table = self.ordered_tables_to_load.pop(0)
        else:
            _LOG.warn("No table to load")
            self.current_table = None

    def _get_locked_chunkfiles(self):
        query = select([self.task.c.database,
                        self.task.c.chunk_id,
                        self.task.c.chunk_file_path,
                        self.task.c.is_overlap,
                        self.task.c.table])
        query = query.where(self.task.c.pod == self.pod)
        result = self.engine.execute(query)
        chunks = result.fetchall()
        return chunks

    def load(self):
        """Load chunks in task queue
           Chunks description should be available at chunks_url
        Returns
        -------
        Integer number
        """

        sql = "DELETE FROM task"
        self.engine.execute(sql)

        for (path, chunk_ids, is_overlap, table) in self.chunk_meta.get_chunk_files_info():
            for chunk_id in chunk_ids:
                _LOG.debug("Add chunk (%s, %s, %s) to queue", chunk_id, table, is_overlap)
                self.engine.execute(
                    self.task.insert(),
                    {"database": self.chunk_meta.database,
                     "chunk_id": chunk_id,
                     "chunk_file_path": path,
                     "is_overlap": is_overlap,
                     "table": table})

    def lock_chunkfiles(self):
        """TODO/FIXME Document
           If chunk files are already locked in queue for current pod, return them
           if not, lock a batch of then and then return their representation,
           or None if chunk file queue is empty
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

                sql = "UPDATE task SET pod = '{}' "
                sql += "WHERE pod IS NULL AND `table` = '{}' "
                sql += "AND `database` = '{}' "
                sql += "ORDER BY chunk_id ASC LIMIT {};"
                query = sql.format(self.pod, self.current_table,
                                   self.chunk_meta.database,
                                   self._chunks_to_lock_number-chunks_locked_count)
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

    def delete_chunks(self):
        """Delete chunks in queue when they have been ingested
           and the super-transaction has been successfully closed
        Returns
        -------
        Integer number, String
        """
        # pod column is UNIQUE index
        logging.debug("Unlock chunk in queue")
        query = delete(self.task)
        query = query.where(self.task.c.pod == self.pod)

        self.engine.execute(query)

    def _is_queue_empty(self):
        return (self.current_table == None)
