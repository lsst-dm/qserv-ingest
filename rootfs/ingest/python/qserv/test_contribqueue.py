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

"""Unit tests for queue.py.

@author  Fabrice Jammes, IN2P3

"""

import datetime
import logging
import os
import time

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from collections.abc import Generator
from typing import Any

import pytest
import yaml

# ----------------------------
# Imports for other modules --
# ----------------------------
from sqlalchemy import (
    Boolean,
    Column,
    DateTime,
    Integer,
    MetaData,
    String,
    Table,
    create_engine,
    event,
    func,
    select,
    update,
)
from sqlalchemy.exc import StatementError

from . import contribqueue, metadata, util
from .ingestconfig import IngestConfig

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

_LOG = logging.getLogger(__name__)

_SCISQL_QUEUE_URL = "sqlite:///testqservingest.db"

_CASE01_DATASET = "case01"

_CHUNK_QUEUE_FRACTION = 10

_DP01_CONTRIBFILES_COUNT = 10
_DP01 = "dp01_dc2_catalogs"


class MockDataAccessLayer:
    connection: Any
    engine: Any
    conn_string = None
    db_meta = MetaData()
    queue = Table(
        "contribfile_queue",
        db_meta,
        Column("id", Integer(), primary_key=True),
        Column("chunk_id", Integer()),
        Column("database", String(50)),
        Column("filepath", String(255)),
        Column("is_overlap", Boolean()),
        Column("table", String(50)),
        Column("locking_pod", String(255), nullable=True),
        Column("succeed", Boolean()),
    )

    mutex = Table(
        "mutex",
        db_meta,
        Column("pod", String(255), nullable=True),
        Column("latest_move", DateTime(), nullable=False),
    )

    def __init__(self, conn_string: str) -> None:
        self.engine = create_engine(conn_string or self.conn_string, future=True)

        @event.listens_for(self.engine, "before_cursor_execute")
        # type: ignore
        def before_cursor_execute(conn, cursor, statement, parameters, context, executemany) -> None:
            _LOG.debug("MOCK DataAccessLayer query: %s", statement)
            _LOG.debug("Parameters:%s", parameters)

    def create_schema(self) -> None:
        self.db_meta.create_all(self.engine)

    def empty_queue(self) -> None:
        delete = self.queue.delete()
        with self.engine.begin() as connection:
            connection.execute(delete)

    def count_contribfiles(self) -> int:
        query = select([func.count()]).select_from(self.queue)
        with self.engine.connect() as connection:
            result = connection.execute(query)
            contrib_count = result.scalar()
            result.close()
        return contrib_count

    def count_locked(self) -> int:
        query = select([func.count()]).select_from(self.queue)
        query = query.where(self.queue.c.locking_pod.isnot(None))
        with self.engine.connect() as connection:
            result = connection.execute(query)
            contrib_locked_count = result.scalar()
            result.close()
        return contrib_locked_count

    def count_succeed(self) -> int:
        query = select([func.count("*")]).select_from(self.queue)
        query = query.where(self.queue.c.succeed.is_(True))
        with self.engine.connect() as connection:
            result = connection.execute(query)
            contrib_locked_count = result.scalar()
            result.close()
        return contrib_locked_count

    def update_all_succeed(self) -> None:
        query = update(self.queue).values(succeed=True)
        with self.engine.begin() as connection:
            connection.execute(query)

    def insert_contribfiles(self) -> None:
        ins = self.queue.insert()
        db = "dp01_dc2_catalogs"
        tbl = "object"
        contrib_files = []
        for i in range(_DP01_CONTRIBFILES_COUNT):
            contrib_file = {
                "chunk_id": 100 + i,
                "database": db,
                "filepath": f"/file{i}.txt",
                "is_overlap": False,
                "table": tbl,
                "succeed": False,
            }
            contrib_files.append(contrib_file)
        db = "mydb"
        for i in range(15):
            contrib_file = {
                "chunk_id": 200 + i,
                "database": db,
                "filepath": f"/file{i}.txt",
                "is_overlap": False,
                "table": tbl,
                "succeed": False,
            }
            contrib_files.append(contrib_file)
        with self.engine.begin() as connection:
            connection.execute(ins, contrib_files)

    def init_mutex(self) -> None:
        delete = self.mutex.delete()
        with self.engine.begin() as connection:
            connection.execute(delete)
            ins = self.mutex.insert()
            mutex = {"pod": None, "latest_move": datetime.datetime.now()}
            connection.execute(ins, mutex)

    def log_queue(self) -> None:
        query = select([self.queue])
        with self.engine.connect() as connection:
            rows = connection.execute(query)
            for row in rows:
                _LOG.info("%s", row)
            rows.close()


@pytest.fixture
def dal() -> Generator[MockDataAccessLayer, None, None]:
    dal = MockDataAccessLayer(_SCISQL_QUEUE_URL)
    yield dal


@pytest.fixture
def init_schema(dal: MockDataAccessLayer) -> None:
    dal.create_schema()
    dal.empty_queue()


@pytest.fixture
def init_queue(dal: MockDataAccessLayer) -> None:
    dal.create_schema()
    dal.empty_queue()
    dal.insert_contribfiles()
    dal.init_mutex()


@pytest.mark.usefixtures("init_schema")
def test_insert_contribfiles() -> None:
    contribfiles_count = 37
    dal = MockDataAccessLayer(_SCISQL_QUEUE_URL)
    data_url = os.path.join(util.DATADIR, _CASE01_DATASET)
    contribution_metadata = metadata.ContributionMetadata(data_url, data_url)
    queue_manager = contribqueue.QueueManager(_SCISQL_QUEUE_URL, contribution_metadata)
    queue_manager.insert_contribfiles()
    count = dal.count_contribfiles()
    assert count == contribfiles_count


@pytest.mark.usefixtures("init_queue")
def test_run_lock_queries() -> None:
    contribfiles_to_lock_count = 3
    dal = MockDataAccessLayer(_SCISQL_QUEUE_URL)
    data_url = os.path.join(util.DATADIR, _DP01)
    contribution_metadata = metadata.ContributionMetadata(data_url, data_url)
    queue_manager = contribqueue.QueueManager(_SCISQL_QUEUE_URL, contribution_metadata)
    dal.log_queue()
    queue_manager._run_lock_queries(contribfiles_to_lock_count)
    count = dal.count_locked()
    dal.log_queue()
    assert count == contribfiles_to_lock_count


@pytest.mark.usefixtures("init_queue")
def test_count_contribfiles() -> None:
    data_url = os.path.join(util.DATADIR, _DP01)
    contribution_metadata = metadata.ContributionMetadata(data_url, data_url)
    queue_manager = contribqueue.QueueManager(_SCISQL_QUEUE_URL, contribution_metadata)
    i = queue_manager._count_contribfiles()
    assert i == _DP01_CONTRIBFILES_COUNT


@pytest.mark.usefixtures("init_queue")
def test_select_noningested_contribfiles() -> None:
    data_url = os.path.join(util.DATADIR, _DP01)
    contribution_metadata = metadata.ContributionMetadata(data_url, data_url)
    queue_manager = contribqueue.QueueManager(_SCISQL_QUEUE_URL, contribution_metadata)
    contribfiles = queue_manager.select_noningested_contribfiles()
    assert len(contribfiles) == _DP01_CONTRIBFILES_COUNT


@pytest.mark.usefixtures("init_queue")
def test_lock_contribfiles() -> None:
    dal = MockDataAccessLayer(_SCISQL_QUEUE_URL)
    contribfiles_to_lock_count = 4
    data_url = os.path.join(util.DATADIR, _DP01)
    contribution_metadata = metadata.ContributionMetadata(data_url, data_url)
    queue_manager = contribqueue.QueueManager(_SCISQL_QUEUE_URL, contribution_metadata)
    queue_manager._contribfiles_to_lock_number = 4
    queue_manager.lock_contribfiles()
    count = dal.count_locked()
    assert count == contribfiles_to_lock_count


def test_unlock_contribfiles() -> None:
    dal = MockDataAccessLayer(_SCISQL_QUEUE_URL)
    contribfiles_to_lock_count = 4
    data_url = os.path.join(util.DATADIR, _DP01)
    contribution_metadata = metadata.ContributionMetadata(data_url, data_url)
    queue_manager = contribqueue.QueueManager(_SCISQL_QUEUE_URL, contribution_metadata)
    queue_manager.unlock_contribfiles(True)
    count = dal.count_succeed()
    dal.log_queue()
    assert count == contribfiles_to_lock_count


@pytest.mark.dev
def test_all_succeed() -> None:
    dal = MockDataAccessLayer(_SCISQL_QUEUE_URL)
    data_url = os.path.join(util.DATADIR, _DP01)
    contribution_metadata = metadata.ContributionMetadata(data_url, data_url)
    queue_manager = contribqueue.QueueManager(_SCISQL_QUEUE_URL, contribution_metadata)
    all_succeed = queue_manager.all_succeed()
    assert all_succeed is False
    dal.update_all_succeed()
    all_succeed = queue_manager.all_succeed()
    dal.log_queue()
    assert all_succeed is True


def test_send_query() -> None:
    data_url = os.path.join(util.DATADIR, _DP01)
    contribution_metadata = metadata.ContributionMetadata(data_url, data_url)
    queue_manager = contribqueue.QueueManager(_SCISQL_QUEUE_URL, contribution_metadata)
    query = update(queue_manager.queue).values(succeed="NOT A BOOLEAN")
    with pytest.raises(StatementError) as e:
        queue_manager._safe_execute(query, 4)
    _LOG.error("Expected error: %s", e)


@pytest.mark.scale
def test_scale_lock_contribfiles() -> None:
    """Used for scale testing purpose, not for unit tests.

    Must be run from inside k8s, against an existing Qserv instances

    Warning: Work in progress

    """
    config_file = os.path.join(util.DATADIR, util.DP02, "ingest.yaml")
    with open(config_file, "r") as values:
        yaml_data = yaml.safe_load(values)

    config = IngestConfig(yaml_data)

    data_url = os.path.join(util.DATADIR, "dp02")
    contribution_metadata = metadata.ContributionMetadata(data_url, data_url)
    queue_manager = contribqueue.QueueManager(config.queue_url, contribution_metadata)

    logging.debug("(Re-)initialize all contributions in queue")
    query = update(queue_manager.queue).values(succeed=None, locking_pod=None)

    queue_manager._safe_execute(query, 0)
    queue_manager.init_mutex()

    queue_manager.set_transaction_size(_CHUNK_QUEUE_FRACTION)
    start_time = time.time()
    for i in range(_CHUNK_QUEUE_FRACTION):
        pod_start_time = time.time()
        queue_manager.pod = f"POD{i}"
        queue_manager.lock_contribfiles()
        pod_total = time.time() - pod_start_time
        _LOG.info("-- Contribfiles locked for pod %s %f", queue_manager.pod, pod_total)
    total = time.time() - start_time
    _LOG.info("-- Total locking time %f", total)

    dal = MockDataAccessLayer(config.queue_url)
    count = dal.count_succeed()
    _LOG.debug("Succeed contrib_file in queue: %s", count)


@pytest.mark.scale
def test_scale_unlock_contribfiles() -> None:
    """Used for scale testing purpose, not for unit tests.

    Must be run from inside k8s, against an existing Qserv instances

    Warning: Work in progress

    """

    config_file = os.path.join(util.DATADIR, util.DP02, "ingest.yaml")
    with open(config_file, "r") as values:
        yaml_data = yaml.safe_load(values)

    config = IngestConfig(yaml_data)

    data_url = os.path.join(util.DATADIR, "dp02")
    contribution_metadata = metadata.ContributionMetadata(data_url, data_url)
    queue_manager = contribqueue.QueueManager(config.queue_url, contribution_metadata)
    start_time = time.time()
    for i in range(_CHUNK_QUEUE_FRACTION):
        pod_start_time = time.time()
        queue_manager.pod = f"POD{i}"
        queue_manager.unlock_contribfiles(True)
        pod_total = time.time() - pod_start_time
        _LOG.info("-- Contribfiles unlocked for pod %s %f", queue_manager.pod, pod_total)
    total = time.time() - start_time
    _LOG.info("-- Total unlocking time %f", total)

    _LOG.debug("Contribfile unlocked")

    dal = MockDataAccessLayer(config.queue_url)
    count = dal.count_succeed()
    _LOG.debug("Succeed contrib_file in queue: %s", count)
