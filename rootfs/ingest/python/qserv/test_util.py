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
import sys

# ----------------------------
# Imports for other modules --
# ----------------------------
import pytest
import qserv.util

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

def test_file_exists():
    """Check if a file exists on a remote HTTP server
    """
    assert qserv.util.http_file_exists("https://www.k8s-school.fr/team/", "index.html")
    assert not qserv.util.http_file_exists("https://www.k8s-school.fr/team/", "false.html")
    
def test_json_get():
    
    data = qserv.util.json_get(__file__,"servers.json")
    assert (data['http_servers'][0] == "https://server1")
    assert (data['http_servers'][2] == "https://server3")