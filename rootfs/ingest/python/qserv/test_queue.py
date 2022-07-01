# This file is part of qserv.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""
Unit tests for queue.py

@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import logging
import os
import pytest

# ----------------------------
# Imports for other modules --
# ----------------------------
from sqlalchemy import (MetaData, Table, Column, Integer, String,
                        Boolean, create_engine, func, select)
from . import queue
from . import metadata

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

_LOG = logging.getLogger(__name__)

_CWD = os.path.dirname(os.path.abspath(__file__))

_QUEUE_URL = 'sqlite:///testqservingest.db'

_CASE01_DATASET = "case01"

_DP01_CONTRIBFILES_COUNT = 10
_DP01_DATABASE = "dp01_dc2_catalogs"


class MockDataAccessLayer:
    connection = None
    engine = None
    conn_string = None
    db_meta = MetaData()
    queue = Table('chunkfile_queue',
                  db_meta,
                  Column('id', Integer(), primary_key=True),
                  Column('chunk_id', Integer()),
                  Column('database', String(50)),
                  Column('filepath', String(255)),
                  Column('is_overlap', Boolean()),
                  Column('table', String(50)),
                  Column('locking_pod', String(255), nullable=True),
                  Column('succeed', Boolean())
                  )

    def __init__(self, conn_string):
        self.engine = create_engine(conn_string or self.conn_string, future=True)
        self.connection = self.engine.connect()

    def __del__(self):
        self.connection.close()

    def create_schema(self):
        self.db_meta.create_all(self.engine)

    def empty_queue(self):
        delete = self.queue.delete()
        self.connection.execute(delete)
        self.connection.commit()

    def count_contribfiles(self) -> int:
        query = select([func.count("*")]).select_from(self.queue)
        result = self.connection.execute(query)
        contrib_count = next(result)[0]
        result.close()
        return contrib_count

    def count_locked(self) -> int:
        query = select([func.count("*")]).select_from(self.queue)
        query = query.where(self.queue.c.locking_pod.is_not(None))
        result = self.connection.execute(query)
        contrib_locked_count = next(result)[0]
        result.close()
        return contrib_locked_count

    def count_succeed(self) -> int:
        query = select([func.count("*")]).select_from(self.queue)
        query = query.where(self.queue.c.succeed.is_(True))
        result = self.connection.execute(query)
        contrib_locked_count = next(result)[0]
        result.close()
        return contrib_locked_count

    def insert_contribfiles(self):
        ins = self.queue.insert()
        db = "dp01_dc2_catalogs"
        tbl = "object"
        contrib_files = []
        for i in range(_DP01_CONTRIBFILES_COUNT):
            contrib_file = {"chunk_id": 100 + i, "database": db, "filepath": f"/file{i}.txt",
                            "is_overlap": True, "table": tbl, "succeed": False}
            contrib_files.append(contrib_file)
        db = "mydb"
        for i in range(15):
            contrib_file = {"chunk_id": 200 + i, "database": db, "filepath": f"/file{i}.txt",
                            "is_overlap": True, "table": tbl, "succeed": False}
            contrib_files.append(contrib_file)
        self.connection.execute(ins, contrib_files)
        self.connection.commit()

    def log_queue(self) -> None:
        query = select(self.queue)
        rows = self.connection.execute(query)
        for row in rows:
            _LOG.info("%s", row)
        rows.close()


@pytest.fixture
def dal():
    dal = MockDataAccessLayer(_QUEUE_URL)
    yield dal


@pytest.fixture
def init_schema(dal):
    dal.create_schema()
    dal.empty_queue()


@pytest.fixture
def init_queue(dal):
    dal.create_schema()
    dal.empty_queue()
    dal.insert_contribfiles()


@pytest.mark.usefixtures("init_schema")
def test_insert_contribfiles():
    contribfiles_count = 37
    dal = MockDataAccessLayer(_QUEUE_URL)
    data_url = os.path.join(_CWD, "testdata", _CASE01_DATASET)
    contribution_metadata = metadata.ContributionMetadata(data_url)
    queue_manager = queue.QueueManager(_QUEUE_URL, contribution_metadata)
    queue_manager.insert_contribfiles()
    count = dal.count_contribfiles()
    assert count == contribfiles_count


@pytest.mark.usefixtures("init_queue")
def test_run_lock_queries():
    contribfiles_to_lock_count = 3
    dal = MockDataAccessLayer(_QUEUE_URL)
    data_url = os.path.join(_CWD, "testdata", _DP01_DATABASE)
    contribution_metadata = metadata.ContributionMetadata(data_url)
    queue_manager = queue.QueueManager(_QUEUE_URL, contribution_metadata)
    queue_manager._run_lock_queries(contribfiles_to_lock_count)
    count = dal.count_locked()
    assert count == contribfiles_to_lock_count


@pytest.mark.usefixtures("init_queue")
def test_count_contribfiles():
    data_url = os.path.join(_CWD, "testdata", _DP01_DATABASE)
    contribution_metadata = metadata.ContributionMetadata(data_url)
    queue_manager = queue.QueueManager(_QUEUE_URL, contribution_metadata)
    i = queue_manager._count_contribfiles()
    assert i == _DP01_CONTRIBFILES_COUNT


@pytest.mark.usefixtures("init_queue")
def test_select_noningested_contribfiles():
    data_url = os.path.join(_CWD, "testdata", _DP01_DATABASE)
    contribution_metadata = metadata.ContributionMetadata(data_url)
    queue_manager = queue.QueueManager(_QUEUE_URL, contribution_metadata)
    contribfiles = queue_manager.select_noningested_contribfiles()
    assert len(contribfiles) == _DP01_CONTRIBFILES_COUNT


@pytest.mark.usefixtures("init_queue")
def test_lock_contribfiles():
    dal = MockDataAccessLayer(_QUEUE_URL)
    contribfiles_to_lock_count = 4
    data_url = os.path.join(_CWD, "testdata", _DP01_DATABASE)
    contribution_metadata = metadata.ContributionMetadata(data_url)
    queue_manager = queue.QueueManager(_QUEUE_URL, contribution_metadata)
    queue_manager._contribfiles_to_lock_number = 4
    queue_manager.lock_contribfiles()
    count = dal.count_locked()
    assert count == contribfiles_to_lock_count


def test_release_locked_contribfiles():
    dal = MockDataAccessLayer(_QUEUE_URL)
    contribfiles_to_lock_count = 4
    data_url = os.path.join(_CWD, "testdata", _DP01_DATABASE)
    contribution_metadata = metadata.ContributionMetadata(data_url)
    queue_manager = queue.QueueManager(_QUEUE_URL, contribution_metadata)
    queue_manager.release_locked_contribfiles(True)
    count = dal.count_succeed()
    dal.log_queue()
    assert count == contribfiles_to_lock_count

