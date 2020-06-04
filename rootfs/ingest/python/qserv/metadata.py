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

# ----------------------------
# Imports for other modules --
# ----------------------------
from .util import download_file

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
METADATA_FILENAME = "metadata.json"

class Metadata():
    """Manage metadata related to data to ingest (database, tables and chunk files)
    """

    def __init__(self, metadata_url):
        """Download metadata located at 'chunks_url' and describing database, tables
           and chunks files, and then load it in a dictionnary.
        """
        self.metadata_url = metadata_url
        metadata_file = download_file(metadata_url, METADATA_FILENAME)
        with open(metadata_file) as json_file:
            self.metadata = json.load(json_file)
    
        db_filename = "{}.json".format(self.metadata['database'])
        db_file = download_file(self.metadata_url, db_file)
        with open(db_file) as json_file:
            self.json_db = json.load(db_file)

        director_table = "{}.json".format(self.metadata['tables']['director'])
        db_file = download_file(self.metadata_url, db_file)
        with open(db_file) as json_file:
            self.metadata_db = json.load(db_file)
