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


"""User-friendly client library for Qserv replication service.

@author  Hsin Fang Chiang, Rubin Observatory
@author  Fabrice Jammes, IN2P3

"""
# -------------------------------
#  Imports of standard modules --
# -------------------------------
import logging
import time
from enum import Enum, auto
from typing import Dict, List, Optional, Tuple

# ----------------------------
# Imports for other modules --
# ----------------------------
from .contribqueue import QueueManager
from .contribution import Contribution
from .exception import IngestError
from .ingestconfig import IngestServiceConfig
from .jsonparser import DatabaseStatus
from .metadata import ContributionMetadata
from .replicationclient import ReplicationClient

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)


class TransactionAction(Enum):
    ABORT_ALL = auto()
    CLOSE = auto()
    CLOSE_ALL = auto()
    LIST_STARTED = auto()
    START = auto()


class Ingester:
    """Manage contribution ingestion tasks Retrieve contribution metadata and
    connection to concurrent queue manager.

    Parameters
    ----------
        contribution_metadata: `ContributionMetadata`
            Metadata for contribution
        replication_url: `str`
            Replication controller URL
        queue_manager: `QueueManager`
            Manager to access contribution queue
    """

    contrib_meta: ContributionMetadata
    """ Contribution metadata, loaded from input data repository """

    def __init__(
        self,
        contribution_metadata: ContributionMetadata,
        replication_url: str,
        timeout_read_sec: int,
        timeout_write_sec: int,
        queue_manager: QueueManager = None,
    ):
        self.timeout_read_sec = timeout_read_sec
        self.timeout_write_sec = timeout_write_sec
        self.contrib_meta = contribution_metadata
        self.queue_manager = queue_manager
        self.repl_client = ReplicationClient(replication_url, self.timeout_read_sec, self.timeout_write_sec)
        Contribution.fileformats = contribution_metadata.fileformats

    def check_sanity(self) -> None:
        """Check
           1. the ingest queue is empty
           2. all transactions for the database are in FINISHED, ABORTED state
           3. no contribution is in progress

        Raises
        ------
        IngestError
            Qserv is not ready for current ingest
        """
        db_status = self.get_database_status()

        match db_status:
            case DatabaseStatus.NOT_REGISTERED:
                _LOG.info(
                    "Database %s can be safely ingested because it has not been previously registered.",
                    self.contrib_meta.database,
                )
            case DatabaseStatus.PUBLISHED:
                raise IngestError(
                    "Database %s is already published and must be deleted "
                    "in order to run the ingest workflow"
                )
            case DatabaseStatus.REGISTERED_NOT_PUBLISHED:
                if self.queue_manager is not None:
                    contributions = self.queue_manager.select_inprogress_contribfiles()
                else:
                    raise IngestError("Uninitialized queue manager")
                raise_msg: str = ""
                if len(contributions) > 0:
                    _LOG.error(f"Contributions in progress from prior database ingest: {contributions}")
                    raise_msg = f"contributions in progress: {len(contributions)}"
                trans = self.repl_client.get_transactions_inprogress(self.contrib_meta.database)
                if len(trans) > 0:
                    raise_msg += f"transactions in progress : {trans}."
                if len(raise_msg) != 0:
                    raise_msg = "Ingest prevented by other workflow instance: " + raise_msg
                    raise IngestError(raise_msg)
                _LOG.info(
                    "Database %s can be safely ingested because no queued contributions are in progress",
                    self.contrib_meta.database,
                )
            case _:
                raise IngestError(f"Unknown status {db_status} for database {self.contrib_meta.database}")

    def check_supertransactions_success(self) -> None:
        """Check all super-transactions have ran successfully."""
        trans = self.repl_client.get_transactions_inprogress(self.contrib_meta.database)
        if len(trans) > 0:
            raise IngestError(f"Database publication prevented by started transactions: {trans}")
        if self.queue_manager is not None:
            contributions = self.queue_manager.select_noningested_contribfiles()
        else:
            raise IngestError("Unitialized queue manager")
        if len(contributions) > 0:
            _LOG.error(f"Non ingested contributions: {contributions}")
            raise IngestError(
                "Database publication forbidden: " + f"non-ingested contributions: {len(contributions)}"
            )
        _LOG.info("All contributions in queue successfully ingested")

    def database_publish(self) -> None:
        """Publish a Qserv database inside replication system."""
        database = self.contrib_meta.database
        self.repl_client.database_publish(database)

    def database_register_and_config(
        self, replication_config: IngestServiceConfig, felis: Optional[Dict] = None
    ) -> None:
        """Register a database, its tables and its configuration inside
        replication/ingest system using data_url/<database_name>.json as input
        data.

        Parameters
        ----------
        replication_config: `util.ReplicationConfig`
            Configuration parameters for the database inside
            replication/ingest system
        felis: `dict`, optional
            Felis schema for tables. Defaults to None.
        """
        self.repl_client.database_register(self.contrib_meta._json_db)
        self.repl_client.database_register_tables(self.contrib_meta.ordered_tables_json, felis)
        self.repl_client.database_config(self.contrib_meta.database, replication_config)

    def deploy_statistics(self) -> None:
        """Build statistics for all tables, using replication client"""

        table_names = self.contrib_meta.table_names
        self.repl_client.deploy_statistics(self.contrib_meta.database, table_names)

    def get_database_status(self) -> DatabaseStatus:
        """Return the status of current Qserv database.

        Returns
        -------
        DatabaseStatus
            Status of current Qserv database.
        """
        return self.repl_client.get_database_status(self.contrib_meta.database, self.contrib_meta.family)

    def ingest(self, contribution_queue_fraction: int) -> None:
        """Ingest contribution for a transaction."""
        if self.queue_manager is not None:
            self.queue_manager.set_transaction_size(contribution_queue_fraction)
        else:
            raise IngestError("Unitialized queue manager")
        has_non_ingested_contributions = True
        while has_non_ingested_contributions:
            has_non_ingested_contributions = self._ingest_transaction()

    def index(self, secondary: bool = False) -> None:
        """Index Qserv MySQL sharded tables or create secondary index."""
        if secondary:
            database = self.contrib_meta.database
            self.repl_client.build_secondary_index(database)
        else:
            json_indexes = self.contrib_meta.json_indexes
            self.repl_client.index_all_tables(json_indexes)

    def _build_contributions(
        self, contribfiles_locked: List[Tuple[str, int, str, bool, str]]
    ) -> List[Contribution]:
        """Build contribution specification using information from:

          - ingest queue for file to be ingested
          - Data Ingest service

        Parameters
        ----------
            contribfiles_locked: `List[Tuple[str, int, str, bool, str]]`
              List of locked contribution files, in queue

        Returns
        -------
            contributions: `List[Contribution]`
              List of contributions to be ingested

        """
        contributions = []
        for contrib_file in contribfiles_locked:
            (database, chunk_id, filepath, is_overlap, table) = contrib_file
            lb_base_url = self.contrib_meta.lb_url
            _charset_name = self.contrib_meta.charset_name
            if chunk_id is not None:
                # Partitioned tables
                (host, port) = self.repl_client.get_chunk_location(chunk_id, database)
                contribution = Contribution(
                    host,
                    port,
                    self.timeout_read_sec,
                    self.timeout_write_sec,
                    chunk_id,
                    filepath,
                    table,
                    is_overlap,
                    lb_base_url,
                    _charset_name,
                )
                contributions.append(contribution)
            else:
                # Regular tables
                locations = self.repl_client.get_regular_tables_locations(database)
                for (host, port) in locations:
                    contribution = Contribution(
                        host,
                        port,
                        self.timeout_read_sec,
                        self.timeout_write_sec,
                        chunk_id,
                        filepath,
                        table,
                        is_overlap,
                        lb_base_url,
                        _charset_name,
                    )
                    contributions.append(contribution)
        return contributions

    def _ingest_transaction(self) -> bool:
        """Get contributions from a queue server for a given database then
        ingest it inside Qserv during a super-transaction.

        Returns
        -------
        continue: `bool`
            0 if no more contribution to load,
            1 if super-transaction was performed successfully

        Raises
        ------
        Raise exception if an error occurs during transaction

        """

        continue_ingest: bool

        if self.queue_manager is None:
            raise IngestError("Unitialized queue manager")
        while True:
            contribfiles_locked = self.queue_manager.lock_contribfiles()
            # Remaining contribution files to ingest
            if len(contribfiles_locked) != 0:
                break
            # No more contribution file to ingest
            # All contribution files have been ingested successfully
            elif self.queue_manager.all_succeed():
                continue_ingest = False
                return continue_ingest
            # No more contribution file to ingest
            # Waiting to recover possibly failed transactions
            else:
                _LOG.info(
                    "Waiting for all contributions managed by other transactions to be in succeed state"
                )
                time.sleep(10)

        transaction_id: Optional[int] = None
        ingest_success: bool = False
        try:
            transaction_id = self.repl_client.start_transaction(self.contrib_meta.database)
            _LOG.info("Start ingest transaction %s", transaction_id)
            contributions = self._build_contributions(contribfiles_locked)

            ingest_success = self._ingest_all_contributions(transaction_id, contributions)
        except Exception as e:
            _LOG.critical("Ingest failed during transaction: %s, %s", transaction_id, e)
            ingest_success = False
            # Stop process when any transaction abort
            raise (e)
        finally:
            if transaction_id is not None:
                if ingest_success:
                    _LOG.info("Close ingest transaction %s", transaction_id)
                elif transaction_id is not None:
                    _LOG.warn("Abort ingest transaction %s", transaction_id)
                self.repl_client.close_transaction(self.contrib_meta.database, transaction_id, ingest_success)
            # Solve error case https://jira.lsstcorp.org/browse/DM-36418:
            # Consider that transaction has not been opened
            # if transaction_id is None and so unlock contribution files
            # in any case (success or failure failure)
            self.queue_manager.unlock_contribfiles(ingest_success)
        continue_ingest = True
        return continue_ingest

    def _ingest_all_contributions(self, transaction_id: int, contributions: list[Contribution]) -> bool:
        """Ingest all contribution for a given transaction. Throw exception if
        ingest fail. This method always returns True, or raises an exception.

        Parameters
        ----------
        transaction_id : `int`
            id of the transaction
        contributions : `list[Contribution]`
            list of contribution to ingest

        Returns
        -------
        success: `bool`
            True if ingest has ran successfully
        """
        loop = True
        _LOG.info(
            "Contributions to ingest during transaction #%s: %s",
            transaction_id,
            len(contributions),
        )
        while loop:
            contribs_started_count = 0
            contribs_notfinished_count = 0
            contribs_prevfinished_count = 0
            contribs_justfinished_count = 0
            for _, c in enumerate(contributions):
                current_time = time.strftime("%H:%M:%S", time.localtime())
                if c.finished:
                    contribs_prevfinished_count += 1
                elif c.request_id is None:
                    # Ingest to start
                    _LOG.debug("Contribution %s ingest started at %s", c, current_time)
                    c.start_async(transaction_id)
                    contribs_started_count += 1
                else:
                    _LOG.debug("Contribution %s ingest monitored at %s", c, current_time)
                    c.finished = c.monitor()
                    if c.finished:
                        # Ingest successfully loaded (i.e. in FINISHED state)
                        _LOG.debug("Contribution %s successfully loaded", c)
                        contribs_justfinished_count += 1
                    else:
                        contribs_notfinished_count += 1

            contribs_unfinished_count = contribs_notfinished_count + contribs_started_count
            if contribs_unfinished_count == 0:
                loop = False
            else:
                _LOG.info(
                    "Contributions for transaction %s, RECENTLY STARTED: %s, NOT FINISHED: %s,"
                    "RECENTLY FINISHED: %s, FINISHED: %s",
                    transaction_id,
                    contribs_started_count,
                    contribs_notfinished_count,
                    contribs_justfinished_count,
                    contribs_prevfinished_count,
                )
                time.sleep(5)

        return True

    def transaction_helper(self, action: TransactionAction, trans_id: int = None) -> None:
        """High-level method which help in managing transaction(s)"""
        database = self.contrib_meta.database
        match action:
            case TransactionAction.ABORT_ALL:
                self.repl_client.abort_all_transactions(database)
            case TransactionAction.START:
                transaction_id = self.repl_client.start_transaction(database)
                _LOG.info("Start transaction %s", transaction_id)
            case TransactionAction.CLOSE:
                if trans_id:
                    self.repl_client.close_transaction(database, trans_id, True)
                    _LOG.info("Commit transaction %s", trans_id)
                else:
                    raise ValueError(f"Missing transaction id for {TransactionAction.CLOSE}")
            case TransactionAction.CLOSE_ALL:
                transaction_ids = self.repl_client.get_transactions_inprogress(database)
                for i in transaction_ids:
                    self.repl_client.close_transaction(database, i, True)
                    _LOG.info("Commit transaction %s", i)
            case TransactionAction.LIST_STARTED:
                self.repl_client.get_transactions_inprogress(database)
