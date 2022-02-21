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

PATH = "/lsst/data/"
SERVERS = [
    "https://server1",
    "https://server2",
    "https://server3"
]
LB_URL = LoadBalancedURL(PATH, SERVERS)

_PARAMS = {"host": "host",
           "port": 8080,
           "chunk_id": 1,
           "path": "step1_1",
           "table": "mytable",
           "is_overlap": True,
           "load_balanced_base_url": LB_URL}


def test_init():
    params = _PARAMS
    contribution = Contribution(**params)

    url = "https://server{}/lsst/data/step1_1/chunk_1_overlap.txt"
    assert(contribution.load_balanced_url.get() == url.format(1))
    assert(contribution.load_balanced_url.get() == url.format(2))
    assert(contribution.load_balanced_url.get() == url.format(3))
    assert(contribution.load_balanced_url.get() == url.format(1))


def test_print():

    c = Contribution(**_PARAMS)
    print(c)
    _LOG.debug(c)

    params = _PARAMS

    params.pop('load_balanced_base_url')
    params['load_balanced_url'] = c.load_balanced_url
    params['request_id'] = None
    params['retry_attempts'] = 0
    params['retry_attempts_post'] = 0
    params['worker_url'] = 'http://host:8080'
    params['ingested'] = False
    expected_string = f"Contribution({params})"
    assert(expected_string == str(c))
