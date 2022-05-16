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
import typing

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


class DatabaseStatus(Enum):
    NOT_REGISTERED = -1
    REGISTERED_NOT_PUBLISHED = 0
    PUBLISHED = 1


class TransactionState(Enum):
    ABORTED = "ABORTED"
    STARTED = "STARTED"
    FINISHED = "FINISHED"


def filter_transactions(responseJson, database, states):
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
                transaction_ids.append(trans["id"])
    return transaction_ids


def get_contribution_status(responseJson: dict) -> ContributionState:
    """Retrieve contribution status"""
    if responseJson["contrib"]["status"] == ContributionState.FINISHED.value:
        return ContributionState.FINISHED
    elif responseJson["contrib"]["status"] == ContributionState.IN_PROGRESS.value:
        return ContributionState.IN_PROGRESS
    elif responseJson["contrib"]["status"] == ContributionState.CANCELLED.value:
        return ContributionState.CANCELLED
    elif responseJson["contrib"]["status"] == ContributionState.CREATE_FAILED.value:
        return ContributionState.CREATE_FAILED
    elif responseJson["contrib"]["status"] == ContributionState.START_FAILED.value:
        return ContributionState.START_FAILED
    elif responseJson["contrib"]["status"] == ContributionState.READ_FAILED.value:
        return ContributionState.READ_FAILED
    elif responseJson["contrib"]["status"] == ContributionState.LOAD_FAILED.value:
        return ContributionState.LOAD_FAILED
    else:
        raise ReplicationControllerError(
            "Unknown contribution status:" + f"{responseJson['contrib']['status']}"
        )


def get_indexes(responseJson, existing_indexes: typing.Dict[str, set] = dict()):
    for worker, data in responseJson["workers"].items():
        table = list(data.keys())[0]
        for idx_data in data[table]:
            try:
                existing_indexes[table].add(idx_data)
            except KeyError:
                existing_indexes[table] = set(idx_data)
    return existing_indexes


def get_location(responseJson: dict) -> typing.Tuple[str, int]:
    """Retrieve chunk location (worker host and port)
    inside json response issued by replication service
    """
    host = responseJson["location"]["http_host"]
    port = int(responseJson["location"]["http_port"])
    return (host, port)


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
