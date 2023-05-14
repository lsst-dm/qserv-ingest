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


"""Helper for ingest contribution management.

@author  Fabrice Jammes, IN2P3

"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import logging
import time
import urllib.parse
from typing import Dict, Optional

from . import metadata

# ----------------------------
# Imports for other modules --
# ----------------------------
from .exception import IngestError
from .http import Http
from .jsonparser import ContributionMonitor, ContributionState, raise_error
from .metadata import FileFormat, LoadBalancedURL
from .util import increase_wait_time

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)

# Max attempts to retry ingesting a file on replication service retriable error
MAX_RETRY_ATTEMPTS = 3


class Contribution:
    """Represent an ingest contribution.

    Store input parameters for replication REST api located at
    http://<worker_host:worker_port>/ingest/file-async

    """

    fileformats: Optional[Dict[str, FileFormat]] = None

    def __init__(
        self,
        worker_host: str,
        worker_port: int,
        chunk_id: int,
        filepath: str,
        table: str,
        is_overlap: bool,
        load_balanced_base_url: LoadBalancedURL,
        charset_name: str = "",
    ):
        self.is_overlap: int
        self.ext: str = ""
        self.chunk_id = chunk_id
        if chunk_id is None:
            # regular tables
            self.chunk_id = -1
        self.table = table
        if is_overlap is None:
            # regular tables
            self.is_overlap = -1
        else:
            # partitioned tables
            self.is_overlap = int(is_overlap)

        for ext in metadata.EXT_LIST:
            if filepath.endswith(ext):
                self.ext = ext

        if len(self.ext) == 0:
            raise IngestError(
                f"Unsupported data format for regular table only {metadata.EXT_LIST} are supported"
            )

        self.charset_name = charset_name
        self.load_balanced_url = LoadBalancedURL.new(load_balanced_base_url, filepath)
        self.request_id = None
        self.worker_url = f"http://{worker_host}:{worker_port}"
        self.finished = False

    def __str__(self) -> str:
        return f"Contribution({self.__dict__})"

    def _build_payload(self, transaction_id: int) -> dict:
        payload = {
            "transaction_id": transaction_id,
            "table": self.table,
            "chunk": self.chunk_id,
            "overlap": self.is_overlap,
            "url": self.load_balanced_url.get(),
            "charset_name": self.charset_name,
        }

        if Contribution.fileformats is not None:
            ff: Optional[FileFormat] = Contribution.fileformats.get(self.ext)
            if ff is not None:
                if ff.fields_enclosed_by is not None:
                    payload["fields_enclosed_by"] = ff.fields_enclosed_by
                if ff.fields_escaped_by is not None:
                    payload["fields_escaped_by"] = ff.fields_escaped_by
                if ff.fields_terminated_by is not None:
                    payload["fields_terminated_by"] = ff.fields_terminated_by
                if ff.lines_terminated_by is not None:
                    payload["lines_terminated_by"] = ff.lines_terminated_by

        return payload

    def start_async(self, transaction_id: int) -> None:
        """Start an asynchronous ingest query for a chunk contribution. Raise
        an exception if the query fails after a fixed number of attempts (see
        MAX_RETRY_ATTEMPTS constant)

        Parameters
        ----------
        transaction_id : `int`
            id of the transaction in which the contribution will be ingested

        """
        url = urllib.parse.urljoin(self.worker_url, "ingest/file-async")
        _LOG.debug("start_async(): url: %s", url)
        payload = self._build_payload(transaction_id)

        _LOG.debug("start_async(): payload: %s", payload)

        responseJson = Http().post_retry(url, payload)

        raise_error(responseJson)
        self.request_id = responseJson["contrib"]["id"]

    def monitor(self) -> bool:
        """Monitor an asynchronous ingest query for a chunk contribution States
        of an ingest query are listed here: https://confluence.lsstcorp.org/dis
        play/DM/Ingest%3A+9.5.3.+Asynchronous+Protocol Propagate an exception
        if the query fails after a fixed number of attempts.

        Raises
        ------
            IngestError
                Raised in case of error during contribution ingest
            ReplicationControllerError
                Raised for unmanaged contribution state or non-retriable errors
                from R-I system

        Returns
        -------
            bool: True if contribution has been successfully ingested
                  False if contribution is being ingested

        """
        status_url = urllib.parse.urljoin(self.worker_url, f"ingest/file-async/{self.request_id}")

        # Retry monitor query if needed
        monitor_request_retry_attempts = 0
        retry = True
        wait_sec = 1
        while retry:
            response_json = Http().get(status_url)
            retry = raise_error(response_json, monitor_request_retry_attempts, MAX_RETRY_ATTEMPTS)
            if retry:
                monitor_request_retry_attempts += 1
                time.sleep(wait_sec)
                # Sleep for longer and longer
                wait_sec = increase_wait_time(wait_sec)

        contrib_monitor = ContributionMonitor(response_json)
        contrib_finished = False
        # For transaction state description
        # see:
        # https://confluence.lsstcorp.org/display/DM/3.+The+asynchronous+protocol
        match contrib_monitor.status:
            case ContributionState.IN_PROGRESS:
                # _LOG.debug("_ingest_chunk: request %s in progress",
                #            self.request_id)
                pass
            case ContributionState.FINISHED:
                contrib_finished = True
            case (
                ContributionState.CREATE_FAILED
                | ContributionState.START_FAILED
                | ContributionState.READ_FAILED
                | ContributionState.LOAD_FAILED
            ):
                msg = (
                    f"Contribution {self} is in status {contrib_monitor.status} "
                    f"with error: {contrib_monitor.error}, "
                    f"system error: {contrib_monitor.system_error}, "
                    f"http error: {contrib_monitor.http_error}"
                )
                noretry_errmsg = ""
                if not contrib_monitor.retry_allowed:
                    noretry_errmsg = "and is not retriable"
                else:
                    noretry_errmsg = "and has exceeded maximum number of ingest retries"
                raise IngestError(f"{msg} {noretry_errmsg}")
            case ContributionState.CANCELLED:
                raise IngestError(f"Contribution {self} ingest has been cancelled by a third-party")
            case _:
                raise IngestError(f"Contribution {self} is in an unmanaged state: {contrib_monitor.status}")

        return contrib_finished
