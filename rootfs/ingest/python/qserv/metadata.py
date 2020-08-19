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
import logging
import urllib.parse

# ----------------------------
# Imports for other modules --
# ----------------------------
from .util import json_get, trailing_slash

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_METADATA_FILENAME = "metadata.json"
_DIRECTOR = "director"

_LOG = logging.getLogger(__name__)


def _get_name(table):
    return table['json']['table']


class ChunkMetadata():
    """Manage metadata related to data to ingest (database, tables and chunk files)
    """

    def __init__(self, data_url):
        """Download metadata located at 'data_url' and describing database, tables
           and chunks files, and then load it in a dictionnary.
        """
        self.data_url = trailing_slash(data_url)

        self.metadata = json_get(self.data_url, _METADATA_FILENAME)
        _LOG.debug("Metadata: %s", self.metadata)

        filename = self.metadata['database']
        self.json_db = json_get(self.data_url, filename)
        self.database = self.json_db['database']
        self.tables = []
        self.init_tables()

    def init_tables(self):
        if self.tables == []:
            for t in self.metadata['tables']:
                table = dict()
                table['data'] = t['data']
                _LOG.debug("Table metadata: %s", t)
                schema_file = t['schema']
                table['json'] = json_get(self.data_url, schema_file)
                idx_files = t['indexes']
                table['indexes'] = []
                for f in idx_files:
                    table['indexes'].append(json_get(self.data_url, f))
                is_director = bool(table['json']['is_director'])
                if is_director:
                    self.tables.insert(0, table)
                else:
                    self.tables.append(table)

    def is_director(self, table_name):
        for t in self.tables:
            if t['json']['table'] == table_name:
                return bool(t['json']['is_director'])
            else:
                return False
        raise Exception("Table '%s' not found", table_name)

    def get_tables_names(self):
        table_names = []
        for t in self.tables:
            table_name = t['json']['table']
            table_names.append(table_name)
        return table_names

    def get_json_indexes(self):
        json_indexes = []
        for t in self.tables:
            for json_idx in t['indexes']:
                json_indexes.append(json_idx)
        return json_indexes

    def get_tables_json(self):
        jsons = []
        for t in self.tables:
            json_data = t['json']
            jsons.append(json_data)
        return jsons

    def get_chunks(self):
        # TODO add iterator over all chunks?
        chunks = []
        for table in self.tables:
            for d in table['data']:
                directory = d['directory']
                url = urllib.parse.urljoin(self.data_url, directory)
                url = trailing_slash(url)
                chunks.append((url, d['chunks'], _get_name(table)))
        return chunks
