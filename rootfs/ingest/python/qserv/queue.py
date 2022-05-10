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
import logging
import socket
import time

# ----------------------------
# Imports for other modules --
# ----------------------------
from .metadata import ContributionMetadata
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
    Class implementing contributions queue manager for Qserv ingest process
    """

    def __init__(self, connection, contribution_metadata: ContributionMetadata):

        db_url = make_url(connection)
        self.engine = sqlalchemy.create_engine(db_url, poolclass=StaticPool, pool_pre_ping=True)
        self.pod = socket.gethostname()

        db_meta = MetaData(bind=self.engine)
        self.queue = Table('chunkfile_queue', db_meta, autoload=True)
        self.contribution_metadata = contribution_metadata
        self.ordered_tables_to_load = self.contribution_metadata.get_tables_names()
        _LOG.debug("Ordered tables to load: %s", self.ordered_tables_to_load)
        self.next_current_table()

    def set_transaction_size(self, contributions_queue_fraction):
        """
        Set number of contributions managed by a single transaction
        """
        contributions_count = self._count_contribution_files_per_database()
        _LOG.debug("Contributions queue size: %s", contributions_count)
        self._contributions_to_lock_number = int(contributions_count / contributions_queue_fraction) + 1

    def _count_contribution_files_per_database(self):
        """
        Count contributions for current database
           if loaded is 'True' count contributions which are not ingested
           else count all contributions.
        """
        query = select([func.count('*')]).select_from(self.queue)
        query = query.where(self.queue.c.database == self.contribution_metadata.database)
        result = self.engine.execute(query)
        contributions_count = next(result)[0]
        result.close()
        return contributions_count

    def next_current_table(self):
        if len(self.ordered_tables_to_load) != 0:
            self.current_table = self.ordered_tables_to_load.pop(0)
        else:
            _LOG.warn("No table to load")
            self.current_table = None

    def _select_locked_contributions(self):
        query = select([self.queue.c.database,
                        self.queue.c.chunk_id,
                        self.queue.c.chunk_file_path,
                        self.queue.c.is_overlap,
                        self.queue.c.table])
        query = query.where(self.queue.c.locking_pod == self.pod)
        query = query.where(self.queue.c.succeed.is_(None))
        query = query.where(self.queue.c.database == self.contribution_metadata.database)
        result = self.engine.execute(query)
        contributions = result.fetchall()
        result.close()
        return contributions

    def load(self):
        """If queue is empty for current database, then load contributions
           in queue else do nothing

        Returns
        -------
        Nothing
        """

        contributions_count = self._count_contribution_files_per_database()
        if contributions_count != 0:
            _LOG.warn("Skip contributions queue load, because it is not empty")
            return

        for (path, chunk_ids, is_overlap, table) in self.contribution_metadata.get_contribution_files_info():
            for chunk_id in chunk_ids:
                _LOG.debug("Add contribution (%s, %s, %s) to queue", chunk_id, table, is_overlap)
                self.engine.execute(
                    self.queue.insert(),
                    {"database": self.contribution_metadata.database,
                     "chunk_id": chunk_id,
                     "chunk_file_path": path,
                     "is_overlap": is_overlap,
                     "table": table})

    def lock_contributions(self) -> list:
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
        contributions_locked = self._select_locked_contributions()
        contributions_locked_count = len(contributions_locked)
        if contributions_locked_count != 0:
            _LOG.debug("Contributions already locked for pod: %s",
                       contributions_locked)
        else:
            _LOG.debug("Current table: %s", self.current_table)
            while (not self._is_queue_empty()
                   and contributions_locked_count < self._contributions_to_lock_number):

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

                contributions_to_lock_count = (self._contributions_to_lock_number -
                                               contributions_locked_count)
                query = self.queue.update(mysql_limit=contributions_to_lock_count).values(locking_pod=self.pod)
                query = query.where(self.queue.c.locking_pod.is_(None))
                query = query.where(self.queue.c.database == self.contribution_metadata.database)
                query = query.where(self.queue.c.table == self.current_table)

                _LOG.debug("Query: %s", query)

                self.engine.execute(query)

                contributions_locked = self._select_locked_contributions()
                contributions_locked_count = len(contributions_locked)
                _LOG.debug("contributions_locked_count: %s", contributions_locked_count)
                if contributions_locked_count < self._contributions_to_lock_number:
                    logging.info("No more contribution to lock for table %s",
                                 self.current_table)
                    self.next_current_table()

        logging.debug("Contributions locked by pod %s: %s",
                      self.pod,
                      contributions_locked)
        return contributions_locked

    def release_locked_contributions(self, ingest_success):
        """ Mark contributions as "succeed" in contribution queue if super-transaction
            has been successfully commited
            Release contributions in queue when the super-transaction has been aborted

            WARN: this operation will be retried until it succeed
            so that contribution queue state is consistent with ingest state
        Returns
        -------
        Integer number, String
        """
        if ingest_success:
            query = self.queue.update().values(succeed=1)
            logging.debug("Mark contributions as 'succeed' in queue")
            query = self.queue.update().values(succeed=1)
        else:
            logging.debug("Unlock contributions in queue")
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

    def get_noningested_contributions(self):
        """Return all contribution in queue not successfully loaded for current database
        """
        query = select([self.queue.c.database,
                        self.queue.c.chunk_id,
                        self.queue.c.chunk_file_path,
                        self.queue.c.is_overlap,
                        self.queue.c.table])
        query = query.where(self.queue.c.succeed.is_(None))
        query = query.where(self.queue.c.database == self.contribution_metadata.database)
        result = self.engine.execute(query)
        contributions = result.fetchall()
        return contributions
