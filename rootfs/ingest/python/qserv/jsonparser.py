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
Parse JSON responses from replication service

@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from enum import Enum
import logging

# ----------------------------
# Imports for other modules --
# ----------------------------
from jsonpath_ng import jsonpath
from jsonpath_ng.ext import parse

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)

class DatabaseStatus(Enum):
    NOT_REGISTERED = -1
    REGISTERED_NOT_PUBLISHED = 0
    PUBLISHED = 1

class TransactionState(Enum):
    ABORTED = "ABORTED"
    STARTED = "STARTED"
    FINISHED = "FINISHED"

def filter_transactions(responseJson, database, states):
    """Filter transactions by state inside json response issued by replication service
    """
    transaction_ids = []
    transactions = responseJson["databases"][database]['transactions']
    _LOG.debug(states)
    if len(transactions) != 0:
        _LOG.debug(f"Transactions for database {database}")
        for trans in transactions:
            _LOG.debug("  id: %s state: %s",
                        trans['id'], trans['state'])
            state = TransactionState(trans['state'])
            if state in states:
                transaction_ids.append(trans['id'])
    return transaction_ids

def get_location(responseJson):
    """ Retrieve chunk location (host and port) inside json response issued by replication service
    """
    host = responseJson["location"]["http_host"]
    port = responseJson["location"]["http_port"]
    return (host, port)

def parse_database_status(responseJson, database, family):
    jsonpath_expr = parse("$.config.families[?(name=\"{}\")].databases[?(name=\"{}\")].is_published".format(family, database))
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