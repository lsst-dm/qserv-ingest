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
Test Contribution class

@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import logging

# ----------------------------
# Imports for other modules --
# ----------------------------
from .contribution import Contribution
from .loadbalancerurl import LoadBalancedURL

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)

_PATH = "/lsst/data/"
_SERVERS = ["https://server1", "https://server2", "https://server3"]
_LB_URL = LoadBalancedURL(_PATH, _SERVERS)

_PARAMS = {
    "worker_host": "host",
    "worker_port": 8080,
    "chunk_id": 1,
    "filepath": "step1_1/chunk_1_overlap.txt",
    "table": "mytable",
    "is_overlap": True,
    "load_balanced_base_url": _LB_URL,
}


def test_init():
    contribution = Contribution(**_PARAMS)

    url = "https://server{}/lsst/data/step1_1/chunk_1_overlap.txt"
    assert contribution.load_balanced_url.get() == url.format(1)
    assert contribution.load_balanced_url.get() == url.format(2)
    assert contribution.load_balanced_url.get() == url.format(3)
    assert contribution.load_balanced_url.get() == url.format(1)


def test_print():

    c = Contribution(**_PARAMS)
    print(c)
    _LOG.debug(c)

    params = _PARAMS

    params.pop("filepath")
    params.pop("load_balanced_base_url")
    params.pop("worker_host")
    params.pop("worker_port")
    params["is_overlap"] = int(params["is_overlap"])
    params["load_balanced_url"] = c.load_balanced_url
    params["request_id"] = None
    params["retry_attempts"] = 0
    params["retry_attempts_post"] = 0
    params["worker_url"] = "http://host:8080"
    params["finished"] = False
    expected_string = f"Contribution({params})"
    assert expected_string == str(c)
