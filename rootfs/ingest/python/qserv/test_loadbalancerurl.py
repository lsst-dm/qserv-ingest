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
@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------

# ----------------------------
# Imports for other modules --
# ----------------------------
from .loadbalancerurl import LoadBalancedURL
import logging


# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

_LOG = logging.getLogger(__name__)


def test_get_loadbalancer_url():
    path = "/lsst-dm/qserv-ingest/master/tests/data/cosmoDC2/"
    servers = [
        "https://server1",
        "https://server2",
        "https://server3"
    ]
    lb_url = LoadBalancedURL(path, servers)
    url = lb_url.get()
    assert (url == "https://server1/lsst-dm/qserv-ingest/master/tests/data/cosmoDC2/")
    url = lb_url.get()
    assert (url == "https://server2/lsst-dm/qserv-ingest/master/tests/data/cosmoDC2/")


def test_join_loadbalancer_url():
    path = "/lsst/data/"
    filename = "file.txt"
    servers = [
        "https://server1",
        "https://server2",
        "https://server3"
    ]
    lb_url = LoadBalancedURL(path, servers)
    lb_url.join(filename)
    url = lb_url.get()
    assert (url == f"https://server1{path}{filename}")