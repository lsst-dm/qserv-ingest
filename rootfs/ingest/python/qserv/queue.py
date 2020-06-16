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
import argparse
import getpass
import json
import logging
import os
import posixpath
import socket
import subprocess
import sys

# ----------------------------
# Imports for other modules --
# ----------------------------
import qserv.util as util
import sqlalchemy
from sqlalchemy import MetaData, Table, Column, Integer
from sqlalchemy.engine.url import make_url
from sqlalchemy.sql import table, column, select, update, insert, delete
from .metadata import ChunkMetadata


# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

_STATUS_IN_PROGRESS = 1
STATUS_COMPLETED = 2

_LOG = logging.getLogger(__name__)


class QueueManager():
    """Class implementing chunk queue manager for Qserv ingest process
    """

    def __init__(self, connection):

        db_url = make_url(connection)
        self.engine = sqlalchemy.create_engine(db_url)
        self.pod = socket.gethostname()

        metadata = MetaData(bind=self.engine)
        self.task = Table('task', metadata, autoload=True)

    def load(self, data_url):
        """Load chunks in task queue
           Chunks description should be available at chunks_url
        Returns
        -------
        Integer number
        """
        data_url = util.trailing_slash(data_url)
        metadata = ChunkMetadata(data_url)

        sql = "DELETE FROM task"
        result = self.engine.execute(sql)

        for (url, chunks, table_type) in metadata.get_chunks():
            for c in chunks:
                _LOG.debug("Insert chunk %s in queue", c)
                result = self.engine.execute(self.task.insert(),
                                             {"database_name": metadata.database,
                                              "chunk_id": c,
                                              "chunk_file_url": url})

    def _get_current_chunk(self):
        # "SELECT chunk_id, chunk_file_url FROM task WHERE pod = ?"
        query = select([self.task.c.database_name,
                        self.task.c.chunk_id, self.task.c.chunk_file_url])
        query = query.where(self.task.c.pod == self.pod)
        result = self.engine.execute(query)
        row = result.first()
        if row:
            chunk = (row[0], int(row[1]), row[2])
        else:
            chunk = None
        return chunk

    def lock_chunk(self):
        """If a chunk is already locked in queue for current pod, get it
           if not, lock it then return its id and file base url, or None if queue is empty
        Returns
        -------
        Integer number, String,  String
        """

        # Check if a chunk was previously locked for this pod
        current_chunk = self._get_current_chunk()

        if not current_chunk:
            sql = "UPDATE task SET pod = '{}', status = {} WHERE pod IS NULL AND status IS NULL ORDER BY chunk_id ASC LIMIT 1;"
            result = self.engine.execute(sql.format(
                self.pod, _STATUS_IN_PROGRESS))

            current_chunk = self._get_current_chunk()

        if not current_chunk:
            logging.info("Chunk queue is empty")
        else:
            logging.debug("Chunk for pod %s: %s", self.pod, current_chunk)
        return current_chunk

    def delete_chunk(self):
        """Delete a chunk in queue when it has been ingested
        Returns
        -------
        Integer number, String
        """
        logging.debug("Unlock chunk in queue")
        query = delete(self.task)
        query = query.where(self.task.c.pod == self.pod)
        result = self.engine.execute(query)
