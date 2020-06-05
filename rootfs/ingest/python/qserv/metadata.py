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
Manage metadata related to input data

@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import json
import logging
import requests

# ----------------------------
# Imports for other modules --
# ----------------------------
from .util import json_response

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
METADATA_FILENAME = "metadata.json"

_LOG = logging.getLogger(__name__)

class ChunkMetadata():
    """Manage metadata related to data to ingest (database, tables and chunk files)
    """

    def __init__(self, metadata_url):
        """Download metadata located at 'chunks_url' and describing database, tables
           and chunks files, and then load it in a dictionnary.
        """
        self.metadata = json_response(metadata_url, METADATA_FILENAME)
        _LOG.debug("Metadata: %s", self.metadata)

        filename = self.metadata['database']
        self.json_db = json_response(metadata_url, filename)
        self.database = self.json_db['database']

        _LOG.debug("Director table: %s", self.metadata['tables'])
        filename = self.metadata['tables']['director']['schema']
        self.json_director = json_response(metadata_url, filename)
        self.dir_director = self.metadata['tables']['director']['datadir']
