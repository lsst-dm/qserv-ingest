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
Helper for ingest contribution management

@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import logging
import urllib.parse

# ----------------------------
# Imports for other modules --
# ----------------------------
from .exception import IngestError,  ReplicationControllerError
from .http import Http
from .jsonparser import ContributionState, get_contribution_status, raise_error
from .metadata import LoadBalancedURL
from .replicationclient import ReplicationClient

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)

# Max attempts to retry ingesting a file on replication service retriable error
MAX_RETRY_ATTEMPTS = 3


def build_contributions(contributions_locked: list,
                        repl_client: ReplicationClient,
                        load_balanced_base_url: LoadBalancedURL) -> list:
    """Build list of contributions to be ingested

    Args:
        contributions_locked (_type_): _description_
        repl_client (_type_): _description_
        load_balanced_url (list): _description_

    Returns:
        list: contributions to be ingested

    """
    contributions = []
    for i, contribution in enumerate(contributions_locked):
        (database, chunk_id, path, is_overlap, table) = contribution

        (host, port) = repl_client.get_chunk_location(chunk_id, database)
        contribution = Contribution(host, port,
                                    chunk_id,
                                    path,
                                    table,
                                    is_overlap,
                                    load_balanced_base_url)
        contributions.append(contribution)
    return contributions


class Contribution():
    """Represent an ingest contribution
    """

    def __init__(self, worker_host, worker_port, chunk_id, path, table, is_overlap,
                 load_balanced_base_url):
        """Store input parameters for replication REST api located at
           http://<worker_host:worker_port>/ingest/file-async
        """
        self.chunk_id = chunk_id
        self.path = path
        self.table = table
        self.is_overlap = is_overlap
        if self.is_overlap:
            filename = f"chunk_{self.chunk_id}_overlap.txt"
        else:
            filename = f"chunk_{self.chunk_id}.txt"
        self.load_balanced_url = load_balanced_base_url.join(path, filename)
        self.request_id = None
        self.retry_attempts = 0
        self.retry_attempts_post = 0
        self.worker_url = f"http://{worker_host}:{worker_port}"
        self.ingested = False

    def __str__(self):
        return f"Contribution({self.__dict__})"

    def start_async(self, transaction_id: int) -> None:
        """Start an asynchronous ingest query for a chunk contribution
           Raise an exception if the query fails after a fixed number of attempts
           (see MAX_RETRY_ATTEMPTS constant)

        Args:
            transaction_id (int): id of the transaction in which the contribution will be ingested

        Returns:
            None request id, returned by the replication controller
        """
        url = urllib.parse.urljoin(self.worker_url, "ingest/file-async")
        _LOG.debug("start_async(): url: %s", url)
        payload = {"transaction_id": transaction_id,
                   "table": self.table,
                   "column_separator": ",",
                   "chunk": self.chunk_id,
                   "overlap": int(self.is_overlap),
                   "url": self.load_balanced_url.get()}
        _LOG.debug("start_async(): payload: %s", payload)

        while not self.request_id:
            # Start ASYNC file ingest request using the POST method.
            # See https://lsstc.slack.com/archives/D2Y1TQY5S/p1645556026791089
            _LOG.debug("_ingest_chunk: url %s, retry attempts: %s, payload: %s",
                       url, self.retry_attempts_post, payload)
            responseJson = Http().post(url, payload)

            retry = raise_error(responseJson, self.retry_attempts_post, MAX_RETRY_ATTEMPTS)
            if retry:
                self.retry_attempts_post += 1
            else:
                self.request_id = responseJson['contrib']['id']

    def monitor(self) -> bool:
        """Monitor an asynchronous ingest query for a chunk contribution
           States of an ingest query are listed here:
           https://confluence.lsstcorp.org/display/DM/Ingest%3A+9.5.3.+Asynchronous+Protocol
           Propagate an exception if the query fails after a fixed number of attempts

        Raises:
            Exception: in case of error during contribution ingest

        Returns:
            bool: True if contribution has been successfully ingested
        """
        status_url = urllib.parse.urljoin(self.worker_url,
                                          f"ingest/file-async/{self.request_id}")
        responseJson = Http().get(status_url)

        contrib_status = get_contribution_status(responseJson)
        match contrib_status:
            case ContributionState.FINISHED:
                self.ingested = True
            case ContributionState.IN_PROGRESS:
                _LOG.debug("_ingest_chunk: request %s in progress", self.request_id)
            case ContributionState.CANCELLED:
                raise IngestError(f'Contribution {self} ingest has been cancelled by a third-party')
            case ContributionState.READ_FAILED:
                if self.retry_attempts >= MAX_RETRY_ATTEMPTS:
                    raise IngestError("Exceeding maximum number of attempts for " +
                                      f"Contribution {self}")
                self.retry_attempts += 1
                self.request_id = None
            case _:
                retry = raise_error(responseJson, self.retry_attempts, MAX_RETRY_ATTEMPTS)
                if retry:
                    self.retry_attempts += 1
                    self.request_id = None
                else:
                    raise ReplicationControllerError(f"Contribution {self}s should be in an error" +
                                                     f" state instead of {contrib_status}")
        return self.ingested
