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
import subprocess
import sys
import urllib.parse

# ----------------------------
# Imports for other modules --
# ----------------------------
import sqlalchemy
from sqlalchemy.engine.url import make_url
from lsst.db import utils

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

class QueueManager():
    """Class implementing chunk queue manager for Qserv ingest process
    """

    def __init__(self, engine):

        self.engine = engine

        self.task = Table('task', metadata,
        Column('chunk_id', Integer, primary_key=True),
        Column('chunk_file_url', Integer, primary_key=True),
        Column('database_name', String),
        Column('pod_name', String),
        Column('status', String),
        Column('timestamp', String),
        )


    def add_chunk(self):

    def lock_chunk(self):
        """Returns current schema version.
        Returns
        -------
        Integer number
        """

        # Initial qservw_worker implementation did not have version number stored at all,
        # and we call this version 0. Since version=1 version number is stored in
        # QMetadata table with key="version"
        if not utils.tableExists(self.engine, "QMetadata"):
            _log.debug("QMetadata missing: version=0")
            return 0
        else:
            query = "SELECT value FROM QMetadata WHERE metakey = 'version'"
            result = self.engine.execute(query)
            row = result.first()
            if row:
                _log.debug("found version in database: %s", row[0])
                return int(row[0])
