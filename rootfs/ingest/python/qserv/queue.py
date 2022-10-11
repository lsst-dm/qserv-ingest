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
from sqlalchemy import MetaData, Table, event, update
from sqlalchemy.exc import OperationalError, StatementError
from sqlalchemy.engine.url import make_url
from sqlalchemy.sql import select, func
from . import util

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

_LOG = logging.getLogger(__name__)

_MAX_RETRY_ATTEMPTS = 100

# remove pylint message for sqlalchemy.Table().insert() method
# see https://github.com/sqlalchemy/sqlalchemy/issues/4656
# noqa pylint: disable=E1120
# noqa pylint: disable=no-value-for-parameter


class QueueManager:
    """
    Class implementing contributions queue manager for Qserv ingest process
    """
    current_table: typing.Optional[str]

    def __init__(self, connection_url: str, contribution_metadata: ContributionMetadata):

        db_url = make_url(connection_url)
        self.engine = sqlalchemy.create_engine(db_url,
                                               pool_recycle=3600,
                                               future=True)

        @event.listens_for(self.engine, "before_cursor_execute")
        # type: ignore
        def before_cursor_execute(conn, cursor, statement, parameters, context, executemany) -> None:
            conn.info.setdefault("query_start_time", []).append(time.time())
            _LOG.debug("Query (400 chars max): %s", statement[:400])
            _LOG.debug("Parameters (first 30):%s", parameters[:30])

        @event.listens_for(self.engine, "after_cursor_execute")
        # type: ignore
        def after_cursor_execute(conn, cursor, statement, parameters, context, executemany) -> None:
            total = time.time() - conn.info["query_start_time"].pop(-1)
            _LOG.debug("Query total time: %f", total)

        self.pod = socket.gethostname()

        db_meta = MetaData(bind=self.engine)
        self.queue = Table("contribfile_queue", db_meta, autoload=True)
        self.mutex = Table("mutex", db_meta, autoload=True)
        self.contribution_metadata = contribution_metadata
        self.ordered_tables_to_load = self.contribution_metadata.get_tables_names()
        _LOG.debug("Ordered tables to load: %s", self.ordered_tables_to_load)
        self._pop_current_table()

    def set_transaction_size(self, contributions_queue_fraction: int) -> None:
        """
        Set number of contributions managed by a single transaction
        """
        contributions_count = self._count_contribfiles()
        _LOG.debug("Contributions queue size: %s", contributions_count)
        self._contribfiles_to_lock_number = int(contributions_count / contributions_queue_fraction) + 1

    def _count_contribfiles(self, not_succeed: bool = False) -> int:
        """
        Count contributions for current database
           if not_succeed is 'True' count contributions which are not ingested
           else count all contributions.
        """
        query = select([func.count("*")]).select_from(self.queue)

        if not_succeed is True:
            query = query.where(self.queue.c.succeed.is_not(True))

        query = query.where(self.queue.c.database == self.contribution_metadata.database)
        with self.engine.connect() as connection:
            result = connection.execute(query)
            contributions_count = next(result)[0]
            result.close()
        return contributions_count

    def _pop_current_table(self) -> None:
        if len(self.ordered_tables_to_load) != 0:
            self.current_table = self.ordered_tables_to_load.pop(0)
        else:
            _LOG.warning("No table to load")
            self.current_table = None

    def all_succeed(self) -> bool:
        """Check all contribution files have beed ingested successfully for current database

        Returns
        -------
        all_succeed : `bool`
            True if all contribution files have beed ingested successfully, else False
        """
        if self._count_contribfiles(not_succeed=True) == 0:
            return True
        else:
            return False

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
        with self.engine.connect() as connection:
            result = connection.execute(query)
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
            with self.engine.begin() as conn:
                conn.execute(self.queue.insert(), contrib_specs)

    def _acquire_mutex(self) -> None:

        query = select([func.count("*")]).select_from(self.mutex)
        with self.engine.connect() as connection:
            result = connection.execute(query)
            size_mutex = next(result)[0]
            result.close()

        if size_mutex != 1:
            raise QueueError("Invalid mutex size", size_mutex)

        loop = True
        wait_sec = 1
        while loop:
            query = select([func.count("*")]).select_from(self.mutex)
            query = query.where(self.mutex.c.pod == self.pod)
            with self.engine.connect() as connection:
                result = connection.execute(query)
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
                with self.engine.begin() as conn:
                    conn.execute(acquire_mutex_query)
                time.sleep(wait_sec)
                # Sleep for longer and longer
                wait_sec = util.increase_wait_time(wait_sec)

    def _release_mutex(self) -> None:
        release_mutex_query = update(self.mutex).values(
            pod=None,
            latest_move=datetime.datetime.now()
        )
        release_mutex_query = release_mutex_query.where(self.mutex.c.pod == self.pod)
        self._safe_execute(release_mutex_query, _MAX_RETRY_ATTEMPTS)

    def init_mutex(self) -> None:
        """Initialize mutex in queue database
           Queue database has a table `mutex` which contain only one row. This row is used to enable
           only one pod to lock contribution file in queue at a time and need to be initialized
           at ingest startup.
        """
        release_mutex_query = update(self.mutex).values(
            pod=None,
            latest_move=datetime.datetime.now()
        )
        self._safe_execute(release_mutex_query, _MAX_RETRY_ATTEMPTS)

    def _run_lock_queries(self, contribfiles_to_lock_count: int) -> int:
        """Assign contribfiles to a pod inside ingest queue

        Parameters
        ----------
        contribfiles_to_lock_count: `int`
            Maximum number of contribfile to lock

        Returns
        -------
        contribfiles_locked_count: int
            Number of contribfiles locked
        """

        try:
            self._acquire_mutex()

            select_query = select([self.queue.c.id])
            select_query = select_query.limit(contribfiles_to_lock_count)
            select_query = select_query.where(self.queue.c.locking_pod.is_(None))
            select_query = select_query.where(self.queue.c.database == self.contribution_metadata.database)

            # _LOG.debug("Query select: %s", select_query.compile(dialect=mysql.dialect()))
            with self.engine.connect() as connection:
                rows = connection.execute(select_query)
                ids = []
                for e in rows:
                    ids.append(e[0])
                rows.close()

            update_query = update(self.queue).values(
                locking_pod=self.pod
            )
            update_query = update_query.where(self.queue.c.id.in_(ids))

            # _LOG.debug("Query (MySQL dialect)): %s", update_query.compile(dialect=mysql.dialect()))
            self._safe_execute(update_query, _MAX_RETRY_ATTEMPTS)
        finally:
            self._release_mutex()
        contribfiles_locked_count = len(ids)
        return contribfiles_locked_count

    def lock_contribfiles(self) -> typing.List[typing.Tuple[str, int, str, bool, str]]:
        """Lock a batch of contribution files and returns their representation,
        return empty list if all contribution have been ingested

        Returns
        -------
        A list of chunk contributions representation where:
        contributions = (string database, int chunk_id, bool is_overlap, string table)
        """

        # Lock contributions for one or more tables
        contribfiles_locked_count_expected = self._run_lock_queries(self._contribfiles_to_lock_number)

        contribfiles_locked = self._select_locked_contribfiles()
        contribfiles_locked_count = len(contribfiles_locked)
        # Non expensive check, should be useless and can be removed in the long term
        if (contribfiles_locked_count_expected != contribfiles_locked_count):
            _LOG.fatal("Unexpected number of locked contributions for pod %s (is: %s, should be: %s)",
                       self.pod, contribfiles_locked_count, contribfiles_locked_count_expected)
        _LOG.debug("contributions_locked_count: %s", contribfiles_locked_count)
        _LOG.debug("%s contribution files locked by pod %s", contribfiles_locked_count, self.pod)

        return contribfiles_locked

    def unlock_contribfiles(self, ingest_success: bool) -> None:
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

        self._safe_execute(query, _MAX_RETRY_ATTEMPTS)

    def _is_queue_empty(self) -> bool:
        if self.current_table is None:
            return True
        else:
            return False

    def select_noningested_contribfiles(self) -> typing.List[typing.Tuple]:
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
        with self.engine.connect() as connection:
            result = connection.execute(query)
            contribfiles = result.fetchall()
            result.close()
        return contribfiles

    def _safe_execute(self, query: typing.Any, max_retry: int = 0) -> None:
        """Retry failed update queries

        Parameters
        ----------
        connection : `Any`
            Sqlalchemy connection
        query : `Any`
            Sql query
        max_retry : `int`
            Maximum number of retry attempts
        """
        wait_sec = 1
        retry_count = 0
        mysql_retry_err_msg = ["MySQL server has gone away", "server closed the connection unexpectedly"]
        sqlalchemy_retry_err_msg = ["Can't reconnect until invalid transaction is rolled back"]
        loop = True
        connection: typing.Any
        while loop:
            retry_count += 1
            try:
                connection = self.engine.connect()
                connection.execute(query)
                connection.commit()
                loop = False
            except OperationalError as ex:
                util.check_raise(ex, mysql_retry_err_msg)
                if retry_count < max_retry:
                    _LOG.error(
                        "Database connection error: {} - sleeping for {}s"
                        " and will retry (attempt #{} of {})".format(
                            ex, wait_sec, retry_count, max_retry
                        )
                    )
                    time.sleep(wait_sec)
                    wait_sec = util.increase_wait_time(wait_sec)
                    continue
                else:
                    raise
            except StatementError as ex:
                util.check_raise(ex, sqlalchemy_retry_err_msg)
                connection.rollback()
                _LOG.error(
                    "An error occurred during execution of a SQL statement: {},"
                    " transaction has been rolled back (attempt #{} of {})".format(
                        ex, retry_count, max_retry
                    )
                )
            finally:
                connection.close()
