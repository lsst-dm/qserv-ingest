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
Tools used by ingest algorithm

@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import json

# ----------------------------
# Imports for other modules --
# ----------------------------
from . import jsonparser
from . import http
import os

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

_CWD = os.path.dirname(os.path.abspath(__file__))
_DATABASE = "cosmoDC2_v1_1_4_image"
_FAMILY = "layout_340_3"

_DATADIR = os.path.join(_CWD, "testdata")


def test_contribution_monitor() -> None:

    response_json = http.json_get(_DATADIR, "response_file_async.json")
    contrib_monitor = jsonparser.ContributionMonitor(response_json)

    assert contrib_monitor.status == jsonparser.ContributionState.LOAD_FAILED
    assert (
        contrib_monitor.error
        == "Connection[119]::execute(_inTransaction=1)  mysql_real_query failed, query: 'LOAD DATA INFILE '/qserv/data/ingest/qservTest_case01_qserv-Logs-4294967295-24-9ad6-c5a6-c537-1086.csv' INTO TABLE `qservTest_case01_qserv`.`Logs`FIELDS TERMINATED BY ',' ESCAPED BY '\\\\' LINES TERMINATED BY '\\n'', error: Data truncated for column 'id' at row 1, errno: 1265"
    )
    assert contrib_monitor.system_error == 11
    assert contrib_monitor.http_error == 0
    assert contrib_monitor.retry_allowed is False


def test_parse_not_finished_transaction() -> None:

    jsonstring = (
        '{"databases": {"cosmoDC2_v1_1_4_image": {"num_chunks": 5, "transactions": ['
        '{"begin_time": 1611956328241, "database": "cosmoDC2_v1_1_4_image", "end_time": 1611956328693, "id": 8, "state": "FINISHED"}, '
        '{"begin_time": 1611956328068, "database": "cosmoDC2_v1_1_4_image", "end_time": 1611956330782, "id": 7, "state": "FINISHED"}, '
        '{"begin_time": 1611956328039, "database": "cosmoDC2_v1_1_4_image", "end_time": 1611956328477, "id": 6, "state": "ABORTED"}, '
        '{"begin_time": 1611956327716, "database": "cosmoDC2_v1_1_4_image", "end_time": 1611956328168, "id": 5, "state": "FINISHED"}, '
        '{"begin_time": 1611956327578, "database": "cosmoDC2_v1_1_4_image", "end_time": 1611956327978, "id": 4, "state": "FINISHED"}, '
        '{"begin_time": 1611956327057, "database": "cosmoDC2_v1_1_4_image", "end_time": 1611956327652, "id": 3, "state": "FINISHED"}, '
        '{"begin_time": 1611956326857, "database": "cosmoDC2_v1_1_4_image", "end_time": 0, "id": 2, "state": "STARTED"}, '
        '{"begin_time": 1611956326351, "database": "cosmoDC2_v1_1_4_image", "end_time": 1611956326806, "id": 1, "state": "ABORTED"}]}},'
        '"error": "", "error_ext": {}, "success": 1}'
    )
    jsondata = json.loads(jsonstring)
    transactions = jsonparser.filter_transactions(jsondata, _DATABASE, [jsonparser.TransactionState.FINISHED])
    assert len(transactions) == 5

    transactions = jsonparser.filter_transactions(jsondata, _DATABASE, [jsonparser.TransactionState.STARTED])
    assert len(transactions) == 1

    transactions = jsonparser.filter_transactions(jsondata, _DATABASE, [jsonparser.TransactionState.ABORTED])
    assert len(transactions) == 2


def test_parse_database_status() -> None:
    response_json = http.json_get(_CWD, "replicationconfig.json")
    status = jsonparser.parse_database_status(response_json, _DATABASE, _FAMILY)
    assert status == jsonparser.DatabaseStatus.REGISTERED_NOT_PUBLISHED
