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
Tools used by ingest algorithm

@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import json
import sys

# ----------------------------
# Imports for other modules --
# ----------------------------
from . import jsonparser
from . import util
import pytest

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

_DATABASE='cosmoDC2_v1_1_4_image'
_FAMILY="layout_340_3"

def test_parse_not_finished_transaction():

    jsonstring=('{"databases": {"cosmoDC2_v1_1_4_image": {"num_chunks": 5, "transactions": ['
        '{"begin_time": 1611956328241, "database": "cosmoDC2_v1_1_4_image", "end_time": 1611956328693, "id": 8, "state": "FINISHED"}, '
        '{"begin_time": 1611956328068, "database": "cosmoDC2_v1_1_4_image", "end_time": 1611956330782, "id": 7, "state": "FINISHED"}, '
        '{"begin_time": 1611956328039, "database": "cosmoDC2_v1_1_4_image", "end_time": 1611956328477, "id": 6, "state": "ABORTED"}, '
        '{"begin_time": 1611956327716, "database": "cosmoDC2_v1_1_4_image", "end_time": 1611956328168, "id": 5, "state": "FINISHED"}, '
        '{"begin_time": 1611956327578, "database": "cosmoDC2_v1_1_4_image", "end_time": 1611956327978, "id": 4, "state": "FINISHED"}, '
        '{"begin_time": 1611956327057, "database": "cosmoDC2_v1_1_4_image", "end_time": 1611956327652, "id": 3, "state": "FINISHED"}, '
        '{"begin_time": 1611956326857, "database": "cosmoDC2_v1_1_4_image", "end_time": 0, "id": 2, "state": "STARTED"}, '
        '{"begin_time": 1611956326351, "database": "cosmoDC2_v1_1_4_image", "end_time": 1611956326806, "id": 1, "state": "ABORTED"}]}},'
        '"error": "", "error_ext": {}, "success": 1}')
    jsondata = json.loads(jsonstring)
    transactions = jsonparser.filter_transactions(jsondata, _DATABASE, [jsonparser.TransactionState.FINISHED])
    assert (len(transactions) == 5)

    transactions = jsonparser.filter_transactions(jsondata, _DATABASE, [jsonparser.TransactionState.STARTED])
    assert (len(transactions) == 1)

    transactions = jsonparser.filter_transactions(jsondata, _DATABASE, [jsonparser.TransactionState.ABORTED])
    assert (len(transactions) == 2)

def test_parse_database_status():
    responseJson = util.json_get(__file__,"replicationconfig.json")
    status = jsonparser.parse_database_status(responseJson, _DATABASE, _FAMILY)
    assert(status == jsonparser.DatabaseStatus.REGISTERED_NOT_PUBLISHED)
