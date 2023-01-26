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

"""Test Contribution class.

@author  Fabrice Jammes, IN2P3

"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import logging
import os
from typing import Dict, TypedDict

from . import metadata, util

# ----------------------------
# Imports for other modules --
# ----------------------------
from .contribution import Contribution
from .loadbalancerurl import LoadBalancedURL, LoadBalancerAlgorithm

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)

_PATH = "/lsst/data/"
_SERVERS = ["https://server1", "https://server2", "https://server3"]

_LBALGO = LoadBalancerAlgorithm(_SERVERS)
_LB_URL = LoadBalancedURL(_PATH, _LBALGO)


class ContribArgs(TypedDict):
    worker_host: str
    worker_port: int
    chunk_id: int
    filepath: str
    table: str
    is_overlap: bool
    load_balanced_base_url: LoadBalancedURL


_PARAMS: ContribArgs = {
    "worker_host": "host",
    "worker_port": 8080,
    "chunk_id": 1,
    "filepath": "step1_1/chunk_1_overlap.txt",
    "table": "mytable",
    "is_overlap": True,
    "load_balanced_base_url": _LB_URL,
}


def test_init() -> None:
    contribution = Contribution(**_PARAMS)

    url = "https://server{}/lsst/data/step1_1/chunk_1_overlap.txt"
    assert contribution.load_balanced_url.get() == url.format(1)
    assert contribution.load_balanced_url.get() == url.format(2)
    assert contribution.load_balanced_url.get() == url.format(3)
    assert contribution.load_balanced_url.get() == url.format(1)


def test_build_payload() -> None:
    transaction_id = 12345
    lbAlgo = LoadBalancerAlgorithm(_SERVERS)
    params: ContribArgs = {
        "worker_host": "host",
        "worker_port": 8080,
        "chunk_id": 1,
        "filepath": "step1_1/chunk_1_overlap.txt",
        "table": "mytable",
        "is_overlap": True,
        "load_balanced_base_url": LoadBalancedURL(_PATH, lbAlgo),
    }

    c = Contribution(**params)
    payload = c._build_payload(transaction_id)

    assert payload["url"] == "https://server1/lsst/data/step1_1/chunk_1_overlap.txt"
    assert payload["url"] == "https://server1/lsst/data/step1_1/chunk_1_overlap.txt"

    params["filepath"] = "step2_2/chunk_2_overlap.txt"
    params["load_balanced_base_url"] = LoadBalancedURL(_PATH, lbAlgo)
    c = Contribution(**params)
    payload = c._build_payload(transaction_id)

    assert payload["url"] == "https://server2/lsst/data/step2_2/chunk_2_overlap.txt"

    params["filepath"] = "step3_3/chunk_3_overlap.txt"
    params["load_balanced_base_url"] = LoadBalancedURL(_PATH, lbAlgo)
    c = Contribution(**params)
    payload = c._build_payload(transaction_id)

    assert payload["url"] == "https://server3/lsst/data/step3_3/chunk_3_overlap.txt"

    params["filepath"] = "step4_4/chunk_4_overlap.txt"
    params["load_balanced_base_url"] = LoadBalancedURL(_PATH, lbAlgo)
    c = Contribution(**params)
    payload = c._build_payload(transaction_id)

    assert payload["url"] == "https://server1/lsst/data/step4_4/chunk_4_overlap.txt"

    data_url = os.path.join(util.DATADIR, "case01")
    contribution_metadata = metadata.ContributionMetadata(data_url)
    Contribution.fileformats = contribution_metadata.fileformats
    c = Contribution(**params)
    payload = c._build_payload(transaction_id)

    assert payload["fields_enclosed_by"] == ""
    assert payload["fields_escaped_by"] == "\\\\"
    assert payload["fields_terminated_by"] == "\\t"
    assert payload["lines_terminated_by"] == "\\n"


def test_print() -> None:
    c = Contribution(**_PARAMS)
    _LOG.debug(c)

    params: Dict = dict()
    params["ext"] = "txt"
    params["chunk_id"] = _PARAMS["chunk_id"]
    params["table"] = _PARAMS["table"]

    if not isinstance(_PARAMS["is_overlap"], int):
        raise ValueError("Unexpected type for params['is_overlap'], should be int")
    else:
        is_overlap = int(_PARAMS["is_overlap"])
    params["is_overlap"] = is_overlap
    params["charset_name"] = ""
    params["load_balanced_url"] = c.load_balanced_url
    params["request_id"] = None
    params["retry_attempts"] = 0
    params["retry_attempts_post"] = 0
    params["worker_url"] = "http://host:8080"
    params["finished"] = False
    expected_string = f"Contribution({params})"
    assert expected_string == str(c)
