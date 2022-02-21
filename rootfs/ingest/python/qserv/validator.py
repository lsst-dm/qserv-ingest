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
from contextlib import redirect_stdout
import difflib
import filecmp
import logging
import os
from pathlib import Path
import shutil
import socket
import subprocess
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
from . import metadata
from . import http
from . import util

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_TESTBENCH_CONFIG = "dbbench.ini"
_TESTBENCH_EXPECTED_RESULTS = "dbbench-expected.tgz"
_TESTBENCH_PATH = "e2e"
_WORKDIR = "/tmp"

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


def _dircmp(dir1: str, dir2: str) -> bool:
    """
    Compare all files in two directories
    Return true if files have the same names and are identical, else false
    """
    comp = filecmp.dircmp(dir1, dir2)
    left = sorted(comp.left_list)
    right = sorted(comp.right_list)
    if left != right:
        _LOG.warning("Query results filenames (%s) are not the expected ones (%s)", left, right)
        return False

    has_same_files = True
    for f in left:
        query_result = os.path.join(dir1, f)
        query_expected_result = os.path.join(dir2, f)
        result = open(query_result, "r")
        expected_result = open(query_expected_result, "r")
        delta = difflib.unified_diff(result.readlines(), expected_result.readlines())
        _LOG.info("Analyze query %s results", f)
        i = 0
        for line in delta:
            i += 1
            _LOG.warning(line)
        if i != 0:
            has_same_files = False
    return has_same_files


class Validator():
    """
    Validate Qserv ingest process has been successful
       - lauch SQL query against currently ingested database
    """

    def __init__(self, contribution_metadata: metadata.ContributionMetadata,
                 query_url: str,
                 sqlEngine: bool = False):
        self.contribution_metadata = contribution_metadata

        self.query_url = util.trailing_slash(query_url)

        if sqlEngine:
            self._initSqlEngine()

    def _initSqlEngine(self):
        database = self.contribution_metadata.database
        qserv_url = make_url(self.query_url)
        if not qserv_url.database and qserv_url.drivername == "mysql":
            qserv_db_url = qserv_url.set(drivername="mariadb+mariadbconnector",
                                         database=database)
        else:
            raise ValueError("Database field in Qserv url must be empty" +
                             " and driver must be mysql: %s",
                             qserv_url)
        self.engine = sqlalchemy.create_engine(qserv_db_url)
        _LOG.debug("Qserv URL: %s", qserv_db_url)

        db_meta = MetaData()
        db_meta.reflect(bind=self.engine)
        self.tables = db_meta.sorted_tables
        _LOG.info("Database: %s, tables: %s", database, self.tables)

    def query(self):
        """
        Lauch simple queries against Qserv database
        using sqlalchemy
        """
        for table in self.tables:
            query = select([func.count()]).select_from(table)
            result = self.engine.execute(query)
            row_count = next(result)[0]
            _LOG.info("Query result: %s", row_count)

    def _download_to_workdir(self, file: str) -> str:
        url = self.contribution_metadata.get_file_url(file)
        if not http.file_exists(url):
            raise FileNotFoundError("File %s does not exist", url)

        local_file_path = os.path.join(_WORKDIR, file)
        http.download_file(url, local_file_path)
        return local_file_path

    def benchmark(self) -> bool:
        """
        Lauch query benchmark against Qserv database
        Return True if query results are as expected, else False
        """
        dbbench_config = self._download_to_workdir(_TESTBENCH_CONFIG)

        dbbench_results_path = os.path.join(_WORKDIR, "dbbench")
        Path(dbbench_results_path).mkdir(parents=True, exist_ok=True)
        dbbench_log = os.path.join(_WORKDIR, "dbbench.log")

        cmd = ['dbbench', '--url', self.query_url,
               '--database', self.contribution_metadata.database, dbbench_config]
        _LOG.info("Run command: %s", ' '.join(cmd))
        with open(dbbench_log, 'wb') as f:
            process = subprocess.Popen(cmd, shell=False,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.STDOUT)
            for c in iter(process.stdout.readline, b''):
                f.write(c)

        if _LOG.isEnabledFor(logging.INFO):
            log = open(dbbench_log, "r")
            for line in log:
                _LOG.info(line)

        # TODO start a thread to download the result archive
        dbbench_expected_results_tgz = self._download_to_workdir(_TESTBENCH_EXPECTED_RESULTS)
        shutil.unpack_archive(dbbench_expected_results_tgz, _WORKDIR)

        dbbench_expected_results_path = os.path.join(_WORKDIR, "dbbench-expected")

        return _dircmp(dbbench_results_path, dbbench_expected_results_path)
