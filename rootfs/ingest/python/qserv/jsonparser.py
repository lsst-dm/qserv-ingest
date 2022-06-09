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
Parse JSON responses from replication service

@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from enum import Enum
import logging
from typing import Dict, List, Tuple

# ----------------------------
# Imports for other modules --
# ----------------------------
from jsonpath_ng.ext import parse


# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
from .exception import ReplicationControllerError

_LOG = logging.getLogger(__name__)


class ContributionState(Enum):
    CANCELLED = "CANCELLED"
    FINISHED = "FINISHED"
    IN_PROGRESS = "IN_PROGRESS"
    CREATE_FAILED = "CREATE_FAILED"
    START_FAILED = "START_FAILED"
    READ_FAILED = "READ_FAILED"
    LOAD_FAILED = "LOAD_FAILED"

    @classmethod
    def from_str(cls, label):
        if label in ("CANCELLED"):
            return cls.CANCELLED
        elif label in ("FINISHED"):
            return cls.FINISHED
        elif label in ("IN_PROGRESS"):
            return cls.IN_PROGRESS
        elif label in ("CREATE_FAILED"):
            return cls.CREATE_FAILED
        elif label in ("START_FAILED"):
            return cls.START_FAILED
        elif label in ("READ_FAILED"):
            return cls.READ_FAILED
        elif label in ("LOAD_FAILED"):
            return cls.LOAD_FAILED
        else:
            raise NotImplementedError(f"Unsupported value: {label} for ContributionState")


class DatabaseStatus(Enum):
    NOT_REGISTERED = -1
    REGISTERED_NOT_PUBLISHED = 0
    PUBLISHED = 1


class TransactionState(Enum):
    ABORTED = "ABORTED"
    STARTED = "STARTED"
    FINISHED = "FINISHED"


class ContributionMonitor:
    """Store contribution status returned by the Ingest Service
    see https://confluence.lsstcorp.org/display/DM/Ingest%3A+9.5.3.+Asynchronous+Protocol
    for details

    Parameters
    ----------
        response_json (dict): Ingest Service response for a HTTP request against URL: ingest/file-async/<request_id>

    Raises
    ------
        ReplicationControllerError: If 'response_json' does not have the expected value or format
    """

    status: ContributionState
    """ Status of the contribution """

    error: str
    """ Error message for the contribution """

    system_error: int
    """ System error code for the contribution """

    http_error: int
    """ HTTP error code for the contribution """

    retry_allowed: bool
    """ True if the contribution can be retried """

    def __init__(self, response_json: dict):

        json_contrib = response_json['contrib']

        if "status" not in json_contrib:
            raise ReplicationControllerError(
                "Missing 'status' field for contribution" + f"{json_contrib}")

        try:
            json_status = response_json["contrib"]["status"]
            self.status = ContributionState.from_str(json_status)
        except NotImplementedError:
            raise ReplicationControllerError(
                f"Unknow status {json_status} for Contribution {json_contrib}")

        if "error" not in response_json["contrib"]:
            raise ReplicationControllerError(
                "Missing 'error' field for contribution" + f"{json_contrib}")

        self.error = json_contrib["error"]

        if "system_error" not in response_json["contrib"]:
            raise ReplicationControllerError(
                "Missing 'system_error' field for contribution" + f"{json_contrib}")

        self.system_error = int(json_contrib["system_error"])

        if "http_error" not in response_json["contrib"]:
            raise ReplicationControllerError(
                "Missing 'http_error' field for contribution" + f"{json_contrib}")

        self.http_error = int(json_contrib["http_error"])

        if "retry_allowed" not in response_json["contrib"]:
            raise ReplicationControllerError(
                "Missing 'retry_allowed' field for contribution" + f"{json_contrib}")

        self.retry_allowed = bool(int(json_contrib["retry_allowed"]))


def filter_transactions(responseJson: Dict, database: str, states: List[TransactionState]) -> List[int]:
    """Filter transactions by state inside json response issued by replication service"""
    transaction_ids = []
    transactions = responseJson["databases"][database]["transactions"]
    _LOG.debug(states)
    if len(transactions) != 0:
        _LOG.debug(f"Transactions for database {database}")
        for trans in transactions:
            _LOG.debug("  id: %s state: %s", trans["id"], trans["state"])
            state = TransactionState(trans["state"])
            if state in states:
                transaction_ids.append(int(trans["id"]))
    return transaction_ids


def get_indexes(responseJson, existing_indexes: Dict[str, set] = dict()):
    for worker, data in responseJson["workers"].items():
        table = list(data.keys())[0]
        for idx_data in data[table]:
            try:
                existing_indexes[table].add(idx_data)
            except KeyError:
                existing_indexes[table] = set(idx_data)
    return existing_indexes


def get_chunk_location(responseJson: dict) -> Tuple[str, int]:
    """Retrieve chunk location (worker host and port)
    inside json response issued by replication service
    """
    host = responseJson["location"]["http_host"]
    port = int(responseJson["location"]["http_port"])
    return (host, port)


def get_regular_table_locations(responseJson: dict) -> List[Tuple[str, int]]:
    """Retrieve locations (workers host and port) for regular tables
    inside json response issued by replication service

    Parameters
    ----------
    responseJson `dict`:
                Response for replication service for the regular table locations API
                see https://confluence.lsstcorp.org/pages/viewpage.action?pageId=133333850#UserguidefortheQservIngestsystem(APIversion8)-Locateregulartables

    Returns
    -------
    locations `typing.List[typing.Tuple[str, int]]`:
                List of connection parameters , for Data ingest Service REST API i.e. http,
                of workers which are available for ingesting regular (fully replicated) tables
    """
    locations = []
    for entry in responseJson["locations"]:
        host = entry["http_host"]
        port = entry["http_port"]
        locations.append((host, port))
    return locations


def parse_database_status(responseJson, database, family):
    jsonpath_expr = parse(
        '$.config.databases[?(database="{}" & family_name="{}")].is_published'.format(
            database, family
        )
    )
    result = jsonpath_expr.find(responseJson)
    if len(result) == 0:
        status = DatabaseStatus.NOT_REGISTERED
    elif len(result) == 1:
        if result[0].value == 0:
            status = DatabaseStatus.REGISTERED_NOT_PUBLISHED
        else:
            status = DatabaseStatus.PUBLISHED
    else:
        raise ValueError("Unexpected answer from replication service", responseJson)
    return status


def raise_error(
    responseJson: dict, retry_attempts: int = -1, max_retry_attempts: int = 0
) -> bool:
    """Check JSON response for error

    Parameters
    ----------
        responseJson (dict): response of a replication controller query, in json format
        retry_attempts (int, optional): number of current retry attempts. Defaults to -1.
        max_retry_attempts (int, optional): number of maximum retry attempts. Defaults to 0.

    Raises
    ------
        ReplicationControllerError
            Raised in case of error in JSON response for a non-retriable request

    Returns
    -------
        bool: True if retry_attempts < max_retry_attempts and if error allows retrying request
              False if no error in JSON response
    """
    if retry_attempts < max_retry_attempts:
        check_retry = True
    else:
        check_retry = False
    is_error_retryable = False
    if not responseJson["success"]:
        _LOG.critical(responseJson["error"])
        error_ext = ""
        if "error_ext" in responseJson:
            _LOG.critical(responseJson["error_ext"])
            error_ext = responseJson["error_ext"]
            if check_retry:
                is_error_retryable = _check_retry(error_ext)
        if not is_error_retryable:
            raise ReplicationControllerError(
                "Error in JSON response", responseJson["error"], error_ext
            )
    return is_error_retryable


def _check_retry(error_ext):
    if "retry_allowed" in error_ext and error_ext["retry_allowed"] != 0:
        retry = True
    else:
        retry = False
    return retry
