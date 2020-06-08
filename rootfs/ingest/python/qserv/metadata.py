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
import urllib.parse

# ----------------------------
# Imports for other modules --
# ----------------------------
from .util import json_response, trailing_slash

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_METADATA_FILENAME = "metadata.json"
_DIRECTOR = "director"

_LOG = logging.getLogger(__name__)



class ChunkMetadata():
    """Manage metadata related to data to ingest (database, tables and chunk files)
    """

    def __init__(self, data_url):
        """Download metadata located at 'data_url' and describing database, tables
           and chunks files, and then load it in a dictionnary.
        """
        self.url = trailing_slash(data_url)

        self.metadata = json_response(self.url, _METADATA_FILENAME)
        _LOG.debug("Metadata: %s", self.metadata)

        filename = self.metadata['database']
        self.json_db = json_response(self.url, filename)
        self.database = self.json_db['database']

        for table in self.metadata['tables']:
            _LOG.debug("Table: %s", table)
            if table['type'] == _DIRECTOR:
                _LOG.debug("Director table: %s", table)
                filename = table['schema']
                self.json_director = json_response(self.url, filename)
                self.data_director = table['data']

    def get_chunks(self):
        #TODO add iterator over all chunks?
        chunks = []
        table = self.data_director
        for d in table['data']:
            directory = d['directory']
            url = urllib.parse.urljoin(self.url, directory)
            url = trailing_slash(url)
            chunks.append((url, table['chunks']), None)
        return chunks
