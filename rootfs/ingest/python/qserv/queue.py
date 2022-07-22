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
Manage a contributions queue used to orchestrate Qserv replication service
on the client side

@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import datetime
import logging
import socket
import time
import typing

# ----------------------------
# Imports for other modules --
# ----------------------------
from .exception import QueueError
from .metadata import ContributionMetadata
import sqlalchemy
from sqlalchemy import MetaData, Table, update
from sqlalchemy.dialects import mysql
from sqlalchemy.engine.url import make_url
from sqlalchemy.pool import StaticPool
from sqlalchemy.sql import select, func
from .util import increase_wait_time

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

_LOG = logging.getLogger(__name__)

# remove pylint message for sqlalchemy.Table().insert() method
# see https://github.com/sqlalchemy/sqlalchemy/issues/4656
# noqa pylint: disable=E1120
# noqa pylint: disable=no-value-for-parameter


class QueueManager:
    """
    Class implementing contributions queue manager for Qserv ingest process
    """

    def __init__(self, connection_url: str, contribution_metadata: ContributionMetadata):

        db_url = make_url(connection_url)
        self.engine = sqlalchemy.create_engine(db_url,
                                               poolclass=StaticPool,
                                               pool_pre_ping=True,
                                               future=True)

        self.connection = self.engine.connect()
        self.pod = socket.gethostname()

        db_meta = MetaData(bind=self.engine)
        self.queue = Table("contribfile_queue", db_meta, autoload=True)
        self.mutex = Table("mutex", db_meta, autoload=True)
        self.contribution_metadata = contribution_metadata
        self.ordered_tables_to_load = self.contribution_metadata.get_tables_names()
        _LOG.debug("Ordered tables to load: %s", self.ordered_tables_to_load)
        self._pop_current_table()

    def __del__(self):
        self.connection.close()

    def set_transaction_size(self, contributions_queue_fraction):
        """
        Set number of contributions managed by a single transaction
        """
        contributions_count = self._count_contribfiles()
        _LOG.debug("Contributions queue size: %s", contributions_count)
        self._contribfiles_to_lock_number = int(contributions_count / contributions_queue_fraction) + 1

    def _count_contribfiles(self) -> int:
        """
        Count contributions for current database
           if loaded is 'True' count contributions which are not ingested
           else count all contributions.
        """
        query = select([func.count("*")]).select_from(self.queue)
        query = query.where(self.queue.c.database == self.contribution_metadata.database)
        result = self.connection.execute(query)
        contributions_count = next(result)[0]
        result.close()
        return contributions_count

    def _pop_current_table(self):
        if len(self.ordered_tables_to_load) != 0:
            self.current_table = self.ordered_tables_to_load.pop(0)
        else:
            _LOG.warning("No table to load")
            self.current_table = None

    def _select_locked_contribfiles(self) -> typing.List[typing.Tuple[str, int, str, bool, str]]:
        query = select(
            [
                self.queue.c.database,
                self.queue.c.chunk_id,
                self.queue.c.filepath,
                self.queue.c.is_overlap,
                self.queue.c.table,
            ]
        )
        query = query.where(self.queue.c.locking_pod == self.pod)
        query = query.where(self.queue.c.succeed.is_(None))
        query = query.where(self.queue.c.database == self.contribution_metadata.database)
        result = self.connection.execute(query)
        contributions = result.fetchall()
        result.close()
        return contributions

    def insert_contribfiles(self) -> None:
        """If queue is empty for current database, then load contribution
           files specification in queue, else do nothing

        Returns
        -------
        Nothing
        """

        contributions_count = self._count_contribfiles()
        if contributions_count != 0:
            _LOG.warn("Skip contributions queue load, because it is not empty")
            return

        for table_contribs_spec in self.contribution_metadata.get_table_contribs_spec():
            contrib_specs = list(table_contribs_spec.get_contrib())
            self.connection.execute(self.queue.insert(), contrib_specs)
            self.connection.commit()

    def _wait_for_mutex(self) -> None:

        query = select([func.count("*")]).select_from(self.mutex)
        result = self.connection.execute(query)
        size_mutex = next(result)[0]
        result.close()

        if size_mutex != 1:
            raise QueueError("Invalid mutex size", size_mutex)

        loop = True
        wait_sec = 1
        while loop:
            query = select([func.count("*")]).select_from(self.mutex)
            query = query.where(self.mutex.c.pod == self.pod)
            result = self.connection.execute(query)
            has_mutex = bool(next(result)[0])
            result.close()

            if has_mutex:
                loop = False
            else:
                acquire_mutex_query = update(self.mutex).values(
                    pod=self.pod,
                    latest_move=datetime.datetime.now()
                )
                acquire_mutex_query = acquire_mutex_query.where(self.mutex.c.pod.is_(None))
                self.connection.execute(acquire_mutex_query)
                self.connection.commit()
                time.sleep(wait_sec)
                # Sleep for longer and longer
                wait_sec = increase_wait_time(wait_sec)

    def _release_mutex(self) -> None:
        release_mutex_query = update(self.mutex).values(
            pod=None,
            latest_move=datetime.datetime.now()
        )
        release_mutex_query = release_mutex_query.where(self.mutex.c.pod == self.pod)
        self.connection.execute(release_mutex_query)
        self.connection.commit()

    def _run_lock_queries(self, contribfiles_to_lock_count: int) -> None:
        # SELECT FOR UPDATE + autocommit=0
        # See:
        # - https://stackoverflow.com/a/18144869/2784039
        # - https://dba.stackexchange.com/questions/311242/mysql-concurrent-updates

        self._wait_for_mutex()

        select_query = select(self.queue.c.id).limit(contribfiles_to_lock_count)
        select_query = select_query.where(self.queue.c.locking_pod.is_(None))
        select_query = select_query.where(self.queue.c.database == self.contribution_metadata.database)
        select_query = select_query.where(self.queue.c.table == self.current_table)

        _LOG.debug("Query select: %s", select_query.compile(dialect=mysql.dialect()))
        rows = self.connection.execute(select_query)

        ids = []
        for e in rows:
            ids.append(e[0])

        update_query = update(self.queue).values(
            locking_pod=self.pod
        )
        update_query = update_query.where(self.queue.c.id.in_(ids))

        _LOG.debug("Query update: %s", update_query.compile(dialect=mysql.dialect()))
        self.connection.execute(update_query)
        self.connection.commit()

        self._release_mutex()

    def lock_contribfiles(self) -> typing.List[typing.Tuple[str, int, str, bool, str]]:
        """If some contributions are already locked in queue for current pod,
        return them
        if not, lock a batch of them and then return their representation,
        return None if all contribution have been ingested

        Returns
        -------
        A list of chunk contributions representation where:
        contributions = (string database, int chunk_id, bool is_overlap, string table)
        """

        # Check contributions which were previously locked for this pod
        contribfiles_locked = self._select_locked_contribfiles()
        contribfiles_locked_count = len(contribfiles_locked)
        if contribfiles_locked_count != 0:
            _LOG.debug("Contribution file already locked for pod: %s", contribfiles_locked)
        else:
            # Lock contributions for one or more tables
            _LOG.debug("Current table: %s", self.current_table)
            while (
                not self._is_queue_empty() and contribfiles_locked_count < self._contribfiles_to_lock_number
            ):

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
                contribfiles_to_lock_count = self._contribfiles_to_lock_number - contribfiles_locked_count

                self._run_lock_queries(contribfiles_to_lock_count)

                contribfiles_locked = self._select_locked_contribfiles()
                contribfiles_locked_count = len(contribfiles_locked)
                _LOG.debug("contributions_locked_count: %s", contribfiles_locked_count)
                if contribfiles_locked_count < self._contribfiles_to_lock_number:
                    previous_table = self.current_table
                    self._pop_current_table()
                    logging.info(
                        "No more contribution to lock for table %s, switch to %s",
                        previous_table,
                        self.current_table,
                    )

        logging.debug("Contribution files locked by pod %s: %s", self.pod, contribfiles_locked)
        return contribfiles_locked

    def release_locked_contribfiles(self, ingest_success: bool) -> None:
        """Mark contributions as "succeed" in contribution queue if super-transaction
        has been successfully commited
        Release contributions in queue when the super-transaction has been aborted

        WARN: this operation will be retried until it succeed
        so that contribution queue state is consistent with ingest state
        """
        if ingest_success:
            logging.debug("Mark contributions as 'succeed' in queue")
            query = update(self.queue).values(succeed=1)
        else:
            logging.debug("Unlock contributions in queue")
            query = update(self.queue).values(locking_pod=None)

        query = query.where(self.queue.c.locking_pod == self.pod)

        wait_sec = 1
        while True:
            _LOG.debug("Query: %s", query)
            self.connection.execute(query)
            try:
                self.connection.commit()
                break
            except Exception as e:
                _LOG.critical("Retry failed query %s, exception: %s", query, e)
                self.connection.rollback()
                time.sleep(wait_sec)
                # Sleep for longer and longer
                wait_sec = increase_wait_time(wait_sec)

    def _is_queue_empty(self) -> bool:
        if self.current_table is None:
            return True
        else:
            return False

    def select_noningested_contribfiles(self):
        """Return all contribution files in queue not successfully loaded for current database"""
        query = select(
            [
                self.queue.c.database,
                self.queue.c.chunk_id,
                self.queue.c.filepath,
                self.queue.c.is_overlap,
                self.queue.c.table,
            ]
        )
        query = query.where(self.queue.c.succeed.is_not(True))
        query = query.where(self.queue.c.database == self.contribution_metadata.database)
        result = self.connection.execute(query)
        contribfiles = result.fetchall()
        return contribfiles
