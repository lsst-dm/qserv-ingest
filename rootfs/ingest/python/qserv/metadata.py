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
Manage metadata related to input data

@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import logging
from typing import List
import urllib.parse


# ----------------------------
# Imports for other modules --
# ----------------------------
from .http import json_get
from .loadbalancerurl import LoadBalancedURL

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


class ContributionMetadata():
    """Manage metadata related to data to ingest:
       database, tables and contribution files
    """

    def __init__(self, path: str, loadbalancers: List[str] = []):
        """Retrieve and store metadata located at 'path' and describing:
             - database
             - tables
             - contribution files

           Support for file:// and http(s):// protocols

        Args:
            path (str): Path to metadata
            loadbalancers (List[str], optional): List of http(s) load balancer urls providing access to metadata. Defaults to [].
        """

        # Get scheme configuration
        self.load_balanced_url = LoadBalancedURL(path, loadbalancers)

        self.metadata_url = self.load_balanced_url.direct_url
        self.metadata = json_get(self.metadata_url, _METADATA_FILENAME)
        _LOG.debug("Metadata: %s", self.metadata)

        filename = self.metadata['database']
        self.json_db = json_get(self.metadata_url, filename)
        self.database = self.json_db['database']
        self.family = "layout_{}_{}".format(self.json_db['num_stripes'],
                                            self.json_db['num_sub_stripes'])
        self._init_tables()

    def get_contribution_files_info(self):
        """
        Retrieve information about input contribution files (CSV formatted)
        in order to insert them inside the contribution files queue

        Returns a list of informations which will allow to retrieve these file
        each entry of the list is a tuple: (<path>, [chunk_ids], <is_overlap>, <table>)
        where [chunk_ids] is the list of the contribution files (XOR overlap) available
        at a given path for a given table and a given chunk
        """
        files_info = []
        for table in self.tables:
            for d in table['data']:
                path = d['directory']
                # TODO simplify algorithm: only director table can have (extra) overlaps
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

    def get_file_url(self, path: str) -> str:
        """
        Return the url of a file located on the input data server
        """
        return urllib.parse.urljoin(self.metadata_url, path)

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

    def get_ordered_tables_json(self):
        """
        Retrieve information about database tables
        in order to register them with the replication service

        Returns a list of json data, one for each table, director tables are at the beginning of the list
        """
        jsons = []
        for t in self.tables:
            json_data = t['json']
            # Director tables need to be loaded first
            if _is_director(t):
                jsons.insert(0, json_data)
            else:
                jsons.append(json_data)
        return jsons

    def _init_tables(self):
        self.tables = []
        self._has_extra_overlaps = False
        for t in self.metadata['tables']:
            table = dict()
            table['data'] = t['data']

            # True if overlap files are different from contribution files,
            # this might occurs if some contribution or overlap files does not exists
            if self._has_extra_overlaps == False:
                for data in table['data']:
                    if data.get(_OVERLAP):
                        self._has_extra_overlaps = True
            _LOG.debug("Table metadata: %s", t)
            schema_file = t['schema']
            table['json'] = json_get(self.metadata_url, schema_file)
            idx_files = t['indexes']
            table['indexes'] = []
            for f in idx_files:
                table['indexes'].append(json_get(self.metadata_url, f))
            if _is_director(table):
                self.tables.insert(0, table)
            else:
                self.tables.append(table)

def _is_director(table):
    return('director_table' in table['json'] and len(table['json']['director_table'])==0)