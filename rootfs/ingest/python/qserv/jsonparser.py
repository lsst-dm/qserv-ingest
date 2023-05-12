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


"""Parse JSON responses from replication service.

@author  Fabrice Jammes, IN2P3

"""

import logging

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from enum import Enum
from typing import Any, Dict, List, Tuple

# ----------------------------
# Imports for other modules --
# ----------------------------
from jsonpath_ng.ext import parse

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
from .exception import IngestError, ReplicationControllerError
from .http import Http

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
    # type: ignore
    def from_str(cls, label: str):
        return ContributionState(label)


class DatabaseStatus(Enum):
    NOT_REGISTERED = -1
    REGISTERED_NOT_PUBLISHED = 0
    PUBLISHED = 1


class TransactionState(Enum):
    ABORTED = "ABORTED"
    ABORT_FAILED = "ABORT_FAILED"
    FINISHED = "FINISHED"
    FINISH_FAILED = "FINISH_FAILED"
    IS_ABORTING = "IS_ABORTING"
    IS_FINISHING = "IS_FINISHING"
    IS_STARTING = "IS_STARTING"
    STARTED = "STARTED"
    START_FAILED = "START_FAILED"


class ContributionMonitor:
    """Store contribution status returned by the Ingest Service see
    https://confluence.lsstcorp.org/display/DM/Ingest%3A+9.5.3.+Asynchronous+Protocol
    for details.

    Parameters
    ----------
    response_json: `dict`
        Ingest Service response for a HTTP request
        against URL: ingest/file-async/<request_id>

    Raises
    ------
    ReplicationControllerError
        Raised if 'response_json' does not have the expected value or format

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

        json_contrib = response_json["contrib"]

        if "status" not in json_contrib:
            raise ReplicationControllerError("Missing 'status' field for contribution" + f"{json_contrib}")

        try:
            json_status = response_json["contrib"]["status"]
            self.status = ContributionState.from_str(json_status)
        except NotImplementedError:
            raise ReplicationControllerError(f"Unknow status {json_status} for Contribution {json_contrib}")

        if "error" not in response_json["contrib"]:
            raise ReplicationControllerError("Missing 'error' field for contribution" f"{json_contrib}")

        self.error = json_contrib["error"]

        if "system_error" not in response_json["contrib"]:
            raise ReplicationControllerError(
                "Missing 'system_error' field for contribution" f"{json_contrib}"
            )

        self.system_error = int(json_contrib["system_error"])

        if "http_error" not in response_json["contrib"]:
            raise ReplicationControllerError("Missing 'http_error' field for contribution" f"{json_contrib}")

        self.http_error = int(json_contrib["http_error"])

        if "retry_allowed" not in response_json["contrib"]:
            raise ReplicationControllerError(
                "Missing 'retry_allowed' field for contribution" f"{json_contrib}"
            )

        self.retry_allowed = bool(int(json_contrib["retry_allowed"]))


def filter_transactions(responseJson: Dict, database: str, states: List[TransactionState]) -> List[int]:
    """Filter transactions by state inside json response issued by replication
    service."""
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


def get_chunk_location(responseJson: dict) -> Tuple[str, int]:
    """Retrieve chunk location (worker host and port) inside json response
    issued by replication service."""
    fqdns = responseJson["location"]["http_host_name"]
    port = int(responseJson["location"]["http_port"])
    fqdn = get_fqdn(fqdns, port)
    if not fqdn:
        raise IngestError(f"Unable to find a valid worker fqdn in json response {responseJson}")
    return (fqdn, port)


def get_fqdn(fqdns: str, port: int, scheme: str = "http") -> str:
    """Return fqdn of the first reachable scheme://fqdn:port entry.

    Parameters
    ----------
    fqdns: `str`
        comma-separated list of fqdns
    port: `int`
        url port to reach

    Returns
    -------
    fqdn : `str`
        First reachable host fqdn, empty string if not fqdn is reachable

    """
    http = Http()
    for fqdn in fqdns.split(","):
        url = f"{scheme}://{fqdn}:{port}"
        if http.is_reachable(url):
            return fqdn
    return ""


def get_regular_table_locations(responseJson: dict) -> List[Tuple[str, int]]:
    """Retrieve locations (workers host and port) for regular tables inside
    json response issued by replication service.

    Parameters
    ----------
    responseJson: `dict`
        Response from replication service API for the regular table locations.
        see
        https://confluence.lsstcorp.org/pages/viewpage.action?pageId=133333850#UserguidefortheQservIngestsystem(APIversion8)-Locateregulartables

    Returns
    -------
    locations `typing.List[typing.Tuple[str, int]]`:
        List of connection parameters for Data ingest Service REST API, for
        workers which are available for ingesting regular (fully replicated)
        tables
    """
    locations = []
    for entry in responseJson["locations"]:
        fqdns = entry["http_host_name"]
        port = entry["http_port"]
        fqdn = get_fqdn(fqdns, port)
        if not fqdn:
            raise IngestError(f"Unable to find a valid worker fqdn in json response {responseJson}")
        locations.append((fqdn, port))
    return locations


def parse_database_status(responseJson: dict, database: str, family: str) -> DatabaseStatus:
    """Retrieve database status inside JSON response from replication
    controller.

    Parameters
    ----------
    responseJson : `dict`
        Response to a replication controller request, in json format
    database : `str`
        Database name
    family : `str`
        Family name of the database

    Returns
    -------
    status : `DatabaseStatus`
        Ingest status of the database

    Raises
    ------
    ValueError
        Raised if JSON response does not contain database name and family

    """
    jsonpath_expr = parse(
        '$.config.databases[?(database="{}" & family_name="{}")].is_published'.format(database, family)
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


def raise_error(responseJson: dict, retry_attempts: int = -1, max_retry_attempts: int = 0) -> bool:
    """Check JSON response from replication controller for error.
    Do not manage retriable errors if retry_attempts != -1
    else manage retriable errors if retry_attempts < max_retry_attempts

    Parameters
    ----------
    responseJson: `dict`
        Response to a replication controller request, in json format
    retry_attempts: `(int, optional)`
        Number of current retry attempts. Defaults to -1.
    max_retry_attempts: `(int, optional)`
        Number of maximum retry attempts. Defaults to 0.

    Raises
    ------
    ReplicationControllerError
        Raised in case of error in JSON response for a non-retriable request

    Returns
    -------
    is_error_retryable: `bool`
        True if retry_attempts < max_retry_attempts and if error allows
        retrying request. False if no error in JSON response.

    """
    if retry_attempts != -1 and retry_attempts < max_retry_attempts:
        check_retry = True
    else:
        check_retry = False
    is_error_retryable = False
    if not responseJson["success"]:
        _LOG.critical(responseJson["error"])
        error_ext: Dict[str, Any] = dict()
        if "error_ext" in responseJson:
            _LOG.critical(responseJson["error_ext"])
            error_ext = responseJson["error_ext"]
            if check_retry:
                is_error_retryable = _check_retry(error_ext)
        if not is_error_retryable:
            raise ReplicationControllerError("Error in JSON response", responseJson["error"], error_ext)
    return is_error_retryable


def _check_retry(error_ext: Dict[str, Any]) -> bool:
    if "retry_allowed" in error_ext and error_ext["retry_allowed"] != 0:
        retry = True
    else:
        retry = False
    return retry
