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
Send SQL queries in order to validate successful ingest for a dataset

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
import sqlalchemy
from sqlalchemy import event, MetaData, Table
from sqlalchemy.engine import Engine
from sqlalchemy.engine.url import make_url
from sqlalchemy.sql import select, delete, func

# ----------------------------
# Imports for other modules --
# ----------------------------
from .metadata import ChunkMetadata
from .util import trailing_slash

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

_LOG = logging.getLogger(__name__)

@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement,
                        parameters, context, executemany):
    conn.info.setdefault('query_start_time', []).append(time.time())
    _LOG.debug("Query: %s", statement)

@event.listens_for(Engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement,
                        parameters, context, executemany):
    total = time.time() - conn.info['query_start_time'].pop(-1)
    _LOG.debug("Query total time: %f", total)


class Validator():
    """Class validating Qserv ingest process has been successful
       - lauch SQL query against currently ingested database
    """

    def __init__(self, chunk_metadata: ChunkMetadata,  qserv_url: str):
        self.chunk_meta = chunk_metadata

        qserv_url = trailing_slash(qserv_url)

        database = self.chunk_meta.database

        qserv_db_url = make_url(qserv_url)
        if not qserv_db_url.database:
            qserv_db_url.database = database
        else:
            raise ValueError("Database field in Qserv url must be empty: %s", qserv_db_url)
        self.engine = sqlalchemy.create_engine(qserv_db_url)
        _LOG.debug("Qserv URL: %s", qserv_db_url)

        db_meta = MetaData(bind=self.engine)
        self.tables = []
        table_names = self.chunk_meta.get_tables_names()
        _LOG.info("Database: %s, tables: %s", database, self.chunk_meta.get_tables_names())
        for table_name in table_names:
            self.tables.append(Table(table_name, db_meta, autoload=True))


    def query(self):
        """Lauch simple queries against Qserv database
        """
        for table in self.tables:
            query = select([func.count()]).select_from(table)
            result = self.engine.execute(query)
            row_count = next(result)[0]
            _LOG.info("Query result: %s", row_count)
