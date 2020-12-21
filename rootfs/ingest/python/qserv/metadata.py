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
import urllib.parse

# ----------------------------
# Imports for other modules --
# ----------------------------
from .util import http_file_exists, json_get, trailing_slash

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_METADATA_FILENAME = "metadata.json"
_DIRECTOR = "director"
_CHUNK = "chunks"
_OVERLAP = 'overlaps'
_FILE_TYPES = [_CHUNK, _OVERLAP]

_LOG = logging.getLogger(__name__)


def _get_name(table):
    return table['json']['table']


class ChunkMetadata():
    """Manage metadata related to data to ingest (database, tables and chunk files)
    """

    def __init__(self, data_url, servers_file = None):
        """Download metadata located at 'data_url' and describing database, tables
           and chunks files, and then load it in a dictionnary.
        """
        self.data_url = trailing_slash(data_url)

        self.metadata = json_get(self.data_url, _METADATA_FILENAME)
        _LOG.debug("Metadata: %s", self.metadata)

        # Get HTTP configuration
        url = urllib.parse.urlsplit(self.data_url, scheme="file")
        self.http_servers = []
        if servers_file and url.scheme in ["http", "https"]:
            with open(servers_file, "r") as f:
                data = json.load(f)
                self.http_servers = data['http_servers']

        filename = self.metadata['database']
        self.json_db = json_get(self.data_url, filename)
        self.database = self.json_db['database']
        self.init_tables()

    def get_chunk_files_info(self):
        # TODO add iterator over all chunks?
        files_info = []
        for table in self.tables:
            for d in table['data']:
                path = d['directory']
                if self._has_extra_overlaps:
                    for ftype in _FILE_TYPES:
                        if d.get(ftype):
                            is_overlap = (ftype == _OVERLAP)
                            files_info.append((path, d[ftype], is_overlap, _get_name(table)))
                else:
                    files_info.append((path, d[_CHUNK], False, _get_name(table)))
                    if _is_director(table):
                        files_info.append((path, d[_CHUNK], True, _get_name(table)))
        return files_info

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

    def init_tables(self):
        self.tables = []
        self._has_extra_overlaps = False
        for t in self.metadata['tables']:
            table = dict()
            table['data'] = t['data']

            # True if overlap files are different from chunk files,
            # this might occurs if some chunk or overlap files are empty
            if self._has_extra_overlaps == False:
                for data in table['data']:
                    if data.get(_OVERLAP):
                        self._has_extra_overlaps = True
            _LOG.debug("Table metadata: %s", t)
            schema_file = t['schema']
            table['json'] = json_get(self.data_url, schema_file)
            idx_files = t['indexes']
            table['indexes'] = []
            for f in idx_files:
                table['indexes'].append(json_get(self.data_url, f))
            if _is_director(table):
                self.tables.insert(0, table)
            else:
                self.tables.append(table)

def _is_director(table):
    return bool(table['json']['is_director'])
