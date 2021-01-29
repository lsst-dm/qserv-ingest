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
from . import metadata
import pytest
import urllib.parse

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

def test_get_loadbalancer_url():
    """Check if a file exists on a remote HTTP server
    """
    data_url = "https://raw.githubusercontent.com/lsst-dm/qserv-ingest/master/data/cosmoDC2_v1_1_4_image_testintegration/"
    server_json = urllib.parse.urljoin(__file__, "servers.json")
    chunk_meta = metadata.ChunkMetadata(data_url, server_json)
    url = chunk_meta.get_loadbalancer_url(1)
    assert (url == "https://server2/lsst-dm/qserv-ingest/master/data/cosmoDC2_v1_1_4_image_testintegration/")
    