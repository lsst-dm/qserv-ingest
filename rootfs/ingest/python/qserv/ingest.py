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
User-friendly client library for Qserv replication service.

@author  Hsin Fang Chiang, Rubin Observatory
@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from enum import Enum, auto
import logging
import sys
from telnetlib import NOP
import time
from typing import List


# ----------------------------
# Imports for other modules --
# ----------------------------
from .contribution import build_contributions
from .exception import IngestError
from .metadata import ContributionMetadata
from .queue import QueueManager
from .replicationclient import ReplicationClient

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
AUTH_PATH = "~/.lsst/qserv"
_LOG = logging.getLogger(__name__)

# Max attempts to retry ingesting a file on replication service retriable error
MAX_RETRY_ATTEMPTS = 3


class TransactionAction(Enum):
    ABORT_ALL = auto()
    CLOSE = auto()
    CLOSE_ALL = auto()
    LIST_STARTED = auto()
    START = auto()


class Ingester():
    """
    Manage contribution ingestion tasks
    """

    def __init__(self, contribution_metadata: ContributionMetadata,
                 replication_url: str,
                 queue_manager: QueueManager = None):
        """Retrieve contribution metadata and connection to concurrent queue manager
        """
        self.contribution_metadata = contribution_metadata
        self.queue_manager = queue_manager
        self.repl_client = ReplicationClient(replication_url)

    def check_supertransactions_success(self):
        """ Check all super-transactions have ran successfully
        """
        trans = self.repl_client.get_transactions_started(self.contribution_metadata.database)
        _LOG.debug(f"IDs of transactions in STARTED state: {trans}")
        if len(trans) > 0:
            raise IngestError(f"Database publication prevented by started transactions: {trans}")
        contributions = self.queue_manager.get_noningested_contributions()
        if len(contributions) > 0:
            _LOG.error(f"Non ingested contributions: {contributions}")
            raise IngestError("Database publication forbidden: " +
                              f"non-ingested contributions: {len(contributions)}")
        _LOG.info("All contributions in queue successfully ingested")

    def database_publish(self):
        """
        Publish a Qserv database inside replication system
        """
        database = self.contribution_metadata.database
        self.repl_client.database_publish(database)

    def database_register_and_config(self, felis=None):
        """
        Register a database, its tables and its configuration inside replication system
        using data_url/<database_name>.json as input data
        """
        self.repl_client.database_register(self.contribution_metadata.json_db)
        self.repl_client.database_register_tables(self.contribution_metadata.get_ordered_tables_json(), felis)
        self.repl_client.database_config(self.contribution_metadata.database)

    def get_database_status(self):
        """
        Return the status of a Qserv catalog database
        """
        return self.repl_client.get_database_status(self.contribution_metadata.database, self.contribution_metadata.family)

    def ingest(self, contribution_queue_fraction):
        """
        Ingest contribution for a transaction
        """
        self.queue_manager.set_transaction_size(contribution_queue_fraction)
        has_non_ingested_contributions = True
        while has_non_ingested_contributions:
            has_non_ingested_contributions = self._ingest_transaction()

    def index(self, secondary=False):
        """
        Index Qserv MySQL sharded tables
        or create secondary index
        """
        if secondary:
            database = self.contribution_metadata.database
            self.repl_client.build_secondary_index(database)
        else:
            json_indexes = self.contribution_metadata.get_json_indexes()
            self.repl_client.index_all_tables(json_indexes)

    def _ingest_transaction(self):
        """
        Get contributions from a queue server for a given database
        then ingest it inside Qserv,
        during a super-transation

        Returns:
        --------
        Integer number: 0 if no more contribution to load,
                        1 if at least one contribution was loaded successfully
        """

        _LOG.info("Start ingest transaction")

        contributions_locked = self.queue_manager.lock_contributions()
        if len(contributions_locked) == 0:
            return False

        transaction_id = None
        try:
            transaction_id = self.repl_client.start_transaction(self.contribution_metadata.database)

            contributions = build_contributions(contributions_locked,
                                                self.repl_client,
                                                self.contribution_metadata.load_balanced_url)
            ingest_success = self._ingest_all_contributions(transaction_id, contributions)
        except Exception as e:
            _LOG.critical('Ingest failed during transaction: %s, %s', transaction_id, e)
            ingest_success = False
            # Stop process when any transaction abort
            raise(e)
        finally:
            if transaction_id:
                self.repl_client.close_transaction(self.contribution_metadata.database,
                                                   transaction_id,
                                                   ingest_success)
                self.queue_manager.release_locked_contributions(ingest_success)

        return True

    def _ingest_all_contributions(self, transaction_id: int, contributions: list) -> bool:
        """Ingest all contribution for a given transaction
           Throw exception if ingest fail
           This method always returns True, or raise an exception

        Args:
            transaction_id (int): id of the transaction
            contributions (list): list of contribution to ingest

        Returns:
            bool: True if ingest has ran successfully
        """
        loop = True
        _LOG.debug('%s contributions to ingest during transaction %s',
                   len(contributions), transaction_id)
        while loop:
            contribs_unfinished_count = 0
            for _, c in enumerate(contributions):
                current_time = time.strftime("%H:%M:%S", time.localtime())
                if c.finished:
                    pass
                elif c.request_id is None:
                    # Ingest to start
                    _LOG.debug('Contribution %s ingest started at %s', c, current_time)
                    c.start_async(transaction_id)
                    contribs_unfinished_count += 1
                else:
                    _LOG.debug('Contribution %s ingest monitored at %s', c, current_time)
                    c.finished = c.monitor()
                    if c.finished:
                        # Ingest successfully loaded (i.e. in FINISHED state)
                        _LOG.debug('Contribution %s successfully loaded', c)
                    else:
                        contribs_unfinished_count += 1

            if contribs_unfinished_count == 0:
                loop = False
            else:
                _LOG.debug('Processing %s contributions for transaction %s',
                           contribs_unfinished_count, transaction_id)
                time.sleep(5)

        return True

    def transaction_helper(self, action: TransactionAction, trans_id: List[int] = None):
        """
        High-level method which help in managing transaction(s)
        """
        database = self.contribution_metadata.database
        if TransactionAction.ABORT_ALL:
            self.repl_client.abort_transactions(database)
        elif TransactionAction.START:
            transaction_id = self.repl_client.start_transaction(database)
            _LOG.info("Start transaction %s", transaction_id)
        elif TransactionAction.CLOSE:
            self.repl_client.close_transaction(database, trans_id, True)
            _LOG.info("Commit transaction %s", trans_id)
        elif TransactionAction.CLOSE_ALL:
            transaction_ids = self.repl_client.get_transactions_started(database)
            for i in transaction_ids:
                self.repl_client.close_transaction(database, i, True)
                _LOG.info("Commit transaction %s", i)
        elif TransactionAction.LIST_STARTED:
            self.repl_client.get_transactions_started(database)
