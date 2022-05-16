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
from dataclasses import dataclass
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
_OVERLAP = "overlaps"
_FILE_TYPES = [_OVERLAP, _CHUNK]

_LOG = logging.getLogger(__name__)


@dataclass
class TableContributionsSpec:
    """Contain contribution specification for a given table
    and available at a given path

     Store informations which will allow to retrieve contributions file
     each entry of the list is a tuple: (<path>, [chunk_ids], <is_overlap>, <table>)
     where [chunk_ids] is the list of the contribution files (XOR overlap) available
     at a given path for a given table and a given chunk
    """

    path: str
    chunks: List[int]
    chunks_overlap: List[int]
    table: str

    def get_contrib(self):
        """Generator for contribution specifications for a given table and a given path

        Yields
        ------
            Iterator[List[dict()]]: Iterator on each contribution specifications for a table
        """
        for id in self.chunks:
            data = {
                "chunk_id": id,
                "chunk_file_path": self.path,
                "is_overlap": False,
                "table": self.table,
            }
            yield data

        for id in self.chunks_overlap:
            data = {
                "chunk_id": id,
                "chunk_file_path": self.path,
                "is_overlap": True,
                "table": self.table,
            }
            yield data


class TableSpec:
    """Contain table specifications for a given database"""

    contrib_specs: List[TableContributionsSpec]
    data: dict()
    is_director: bool
    is_partitioned: bool
    json_indexes: List[str]
    json_schema: dict()
    _name: str

    def __init__(self, metadata_url: str, table_meta: str):
        self.data = table_meta["data"]
        schema_file = table_meta["schema"]
        self.json_schema = json_get(metadata_url, schema_file)
        self._name = self.json_schema["table"]
        self.is_partitioned = self.json_schema["is_partitioned"] == 1
        self.is_director = (
            "director_table" in self.json_schema
            and len(self.json_schema["director_table"]) == 0
        )
        idx_files = table_meta["indexes"]
        self.json_indexes = []
        for f in idx_files:
            self.json_indexes.append(json_get(metadata_url, f))

        self.contrib_specs = []
        for d in self.data:
            path = d["directory"]
            chunks = d[_CHUNK]
            chunks_overlap = []
            # Only director tables can have (extra) overlaps
            if self.is_director:
                # chunk ids for overlaps migh be different of regular chunk ids
                if d.get(_OVERLAP):
                    chunks_overlap = d[_OVERLAP]
                else:
                    chunks_overlap = d[_CHUNK]
            self.contrib_specs.append(
                TableContributionsSpec(path, chunks, chunks_overlap, self._name)
            )


class ContributionMetadata:
    """Manage metadata related to data to ingest:
    database, tables and contribution files
    """

    tables: List[TableSpec]

    def __init__(self, path: str, loadbalancers: List[str] = []):
        """Retrieve and store metadata located at 'path' and describing:
             - database
             - tables
             - contribution files

           Support for file:// and http(s):// protocols

        Parameters
        ----------
            path (str): Path to metadata
            loadbalancers (List[str], optional): List of http(s) load balancer urls providing
            access to metadata. Defaults to [].
        """

        # Get scheme configuration
        self.load_balanced_url = LoadBalancedURL(path, loadbalancers)

        self.metadata_url = self.load_balanced_url.direct_url
        self.metadata = json_get(self.metadata_url, _METADATA_FILENAME)

        filename = self.metadata["database"]
        self.json_db = json_get(self.metadata_url, filename)
        self.database = self.json_db["database"]
        self.family = "layout_{}_{}".format(
            self.json_db["num_stripes"], self.json_db["num_sub_stripes"]
        )
        self._init_tables()

    def get_contribution_specs(self):
        """Generator for contribution specifications for the whole database

        Retrieve information about input contribution files
        in order to insert them inside the contribution files queue

        Yields
        ------
            Iterator[List[dict()]]: Iterator on each contribution specifications for a database
        """
        for table in self.tables:
            for table_contrib_spec in table.contrib_specs:
                yield table_contrib_spec

    def get_file_url(self, path: str) -> str:
        """
        Return the url of a file located on the input data server
        """
        return urllib.parse.urljoin(self.metadata_url, path)

    def get_tables_names(self):
        table_names = []
        for t in self.tables:
            table_name = t["json_schema"]["table"]
            table_names.append(table_name)
        return table_names

    def get_json_indexes(self):
        json_indexes = []
        for t in self.tables:
            for json_idx in t["indexes"]:
                json_indexes.append(json_idx)
        return json_indexes

    def get_ordered_tables_json(self) -> List[str]:
        """Retrieve json schema for each tables of a given database
        in order to register them inside the replication service

        Returns
        -------
        l: List[str]
            a list of json schemas in the R-I system format, one for each table,
            director tables are at the beginning of the list
        """
        schema_files = []
        for t in self.tables:
            schema_files.append(t.json_schema)
        return schema_files

    def _init_tables(self):
        self.tables = []
        self._has_extra_overlaps = False
        for table_meta in self.metadata["tables"]:
            table = TableSpec(self.metadata_url, table_meta)
            if table.is_director:
                self.tables.insert(0, table)
            else:
                self.tables.append(table)
