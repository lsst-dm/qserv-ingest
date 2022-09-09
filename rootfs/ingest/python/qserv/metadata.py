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
from collections.abc import Generator
from dataclasses import dataclass
import logging
from typing import Any, Dict, List, Optional
import urllib.parse


# ----------------------------
# Imports for other modules --
# ----------------------------
from .http import json_get
from .loadbalancerurl import LoadBalancerAlgorithm, LoadBalancedURL

CSV = 'csv'
TSV = 'tsv'
TXT = 'txt'
EXT_LIST: List[str] = [CSV, TSV, TXT]
"""List of supported input data file extensions
"""

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_CHUNKS: str = "chunks"
_FILES: str = "files"
_METADATA_FILENAME: str = "metadata.json"
_OVERLAPS: str = "overlaps"
_LOG = logging.getLogger(__name__)


class FileFormat(object):
    """Define input data file format for mariadb 'LOAD DATA INFILE' statement
    see https://mariadb.com/kb/en/load-data-infile/

    Parameters
    ----------
    fields_enclosed_by : `Optional[str]`
        Set mariadb "FIELDS ENCLOSED BY" option,
        default to 'None' and then use replication service default
    fields_escaped_by : `Optional[str]`
        Set mariadb "FIELDS ESCAPED BY" option,
        default to 'None' and then use replication service default
    fields_terminated_by: `Optional[str]`
        Set mariadb "FIELDS TERMINATED BY" option,
        default to 'None' and then use replication service default
    lines_terminated_by: `Optional[str]`
        Set mariadb "LINES TERMINATED BY" option,
        default to 'None' and then use replication service default
    """

    def __init__(self, fields_enclosed_by: Optional[str] = None, fields_escaped_by: Optional[str] = None,
                 fields_terminated_by: Optional[str] = None, lines_terminated_by: Optional[str] = None):
        self.fields_enclosed_by = fields_enclosed_by
        self.fields_escaped_by = fields_escaped_by
        self.fields_terminated_by = fields_terminated_by
        self.lines_terminated_by = lines_terminated_by


@dataclass
class TableContributionsSpec:
    """Contain contribution specification for a given table
    and for a given path.

     Store informations which will allow to retrieve contributions file.
     Each entry of the list is a tuple: (<path>, [chunk_ids], <is_overlap>, <table>)
     where [chunk_ids] is the list of the contribution files (XOR overlap) available
     at a given path for a given table and a given chunk.
    """

    base_path: str
    """ Base path """

    database: str
    """ Database name """

    table: str
    """ Table name """

    files: List[str]
    """ Files for regular tables, empty for partitioned tables """

    chunks: List[int]
    """ Chunks ids for files for partitioned tables, empty for non-partitioned tables """

    chunks_overlap: List[int]
    """ Chunks ids for overlap files for partioned tables, empty for non-partitioned tables """

    def get_contrib(self) -> Generator[Dict[str, Any], None, None]:
        """Generator for contribution specifications for a given table and a given path

        Yields
        ------
            Iterator[List[dict()]]: Iterator on each contribution specifications for a table
        """
        data: Dict[str, Any]
        for file in self.files:
            data = {
                "chunk_id": None,
                "database": self.database,
                "filepath": self._filepath(file),
                "is_overlap": None,
                "table": self.table,
            }
            yield data

        for id in self.chunks:
            data = {
                "chunk_id": id,
                "database": self.database,
                "filepath": self._filepath(f"chunk_{id}.txt"),
                "is_overlap": False,
                "table": self.table,
            }
            yield data

        for id in self.chunks_overlap:
            data = {
                "chunk_id": id,
                "database": self.database,
                "filepath": self._filepath(f"chunk_{id}_overlap.txt"),
                "is_overlap": True,
                "table": self.table,
            }
            yield data

    def _filepath(self, filename: str) -> str:
        filepath = self.base_path.strip("/") + "/" + filename.strip("/")
        return filepath


class TableSpec:
    """Contain table specifications

    Parameters:
    -----------
    metadata_url : `str`
        url of metadata, used to access tables' json configuration files for R-I service
    table_meta : `Dict`
        metadata for a table
    """

    def __init__(self, metadata_url: str, table_meta: Dict):

        self.data: List[Any] = table_meta["data"]
        schema_file: str = table_meta["schema"]
        self.json_schema: Dict[Any, Any] = json_get(metadata_url, schema_file)
        self.name: str = self.json_schema["table"]
        self.database: str = self.json_schema["database"]
        self.is_partitioned: bool = self.json_schema["is_partitioned"] == 1
        self.is_director: bool = self._is_director()
        idx_files: List(str) = table_meta.get("indexes", [])
        self.json_indexes: List[Dict[str, Any]] = []
        for f in idx_files:
            self.json_indexes.append(json_get(metadata_url, f))

        self.contrib_specs: List[TableContributionsSpec] = []
        for d in self.data:
            path = d["directory"]
            chunks = []
            chunks_overlap = []
            files = []
            if self.is_partitioned:
                chunks = d[_CHUNKS]
                # Only director tables can have (extra) overlaps
                if self.is_director:
                    # chunk ids for overlaps might be different of regular chunk ids
                    if d.get(_OVERLAPS):
                        chunks_overlap = d[_OVERLAPS]
                    else:
                        chunks_overlap = d[_CHUNKS]
            else:
                files = d[_FILES]
            self.contrib_specs.append(TableContributionsSpec(path, self.database, self.name, files,
                                                             chunks, chunks_overlap))

    def _is_director(self) -> bool:
        is_director: bool = False
        director_table = self.json_schema.get("director_table")
        if director_table is not None:
            if len(director_table) == 0:
                is_director = True
        elif self.json_schema.get("is_partitioned") == 1:
            is_director = True
        return is_director


class ContributionMetadata:
    """Manage metadata related to data to ingest:
    database, tables and contribution files
    """

    def __init__(self, path: str, loadbalancers: List[str] = []):
        """Retrieve and store metadata located at 'path' and describing:
             - database
             - tables
             - contribution files

           Support for file:// and http(s):// protocols

        Parameters
        ----------
        path : `str`
            Path to metadata
        loadbalancers: `List[str]` optional
            List of http(s) load balancer urls providing access to metadata. Defaults to [].
        """

        self.fileformats: Dict[str, FileFormat] = {}
        self.tables: List[TableSpec]

        # Get scheme configuration
        lbAlgo = LoadBalancerAlgorithm(loadbalancers)
        self.lb_url = LoadBalancedURL(path, lbAlgo)

        self.metadata_url = self.lb_url.direct_url
        self.metadata = json_get(self.metadata_url, _METADATA_FILENAME)

        filename = self.metadata["database"]
        self.json_db = json_get(self.metadata_url, filename)
        self.database = self.json_db["database"]
        self.family = "layout_{}_{}".format(self.json_db["num_stripes"], self.json_db["num_sub_stripes"])
        self._init_tables()
        self._init_fileformats()

    def get_table_contribs_spec(self) -> Generator[TableContributionsSpec, None, None]:
        """Generator for contribution specifications for the whole database

        Retrieve information about input contribution files
        in order to insert them inside the contribution files queue

        Yields
        ------
            Iterator[List[dict()]]: Iterator on each contribution specifications for a database
        """
        for table in self.tables:
            yield from table.contrib_specs

    def get_file_url(self, path: str) -> str:
        """
        Return the url of a file located on the input data server
        """
        return urllib.parse.urljoin(self.metadata_url, path)

    def get_tables_names(self) -> List[str]:
        table_names = []
        for t in self.tables:
            table_names.append(t.name)
        return table_names

    def get_json_indexes(self) -> List[Dict[str, Any]]:
        json_indexes: List[Dict] = []
        for tbl in self.tables:
            json_indexes.extend(tbl.json_indexes)
        return json_indexes

    def get_ordered_tables_json(self) -> List[Dict[Any, Any]]:
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

    def _init_tables(self) -> None:
        self.tables = []
        self._has_extra_overlaps = False
        for table_meta in self.metadata["tables"]:
            table = TableSpec(self.metadata_url, table_meta)
            if table.is_director:
                self.tables.insert(0, table)
            else:
                self.tables.append(table)

    def _init_fileformats(self) -> None:
        format = self.metadata.get('formats')
        if format:
            for ext in EXT_LIST:
                format_spec = format.get(ext)
                if format_spec:
                    self.fileformats[ext] = FileFormat(**format_spec)

        for ext in EXT_LIST:
            if self.fileformats.get(ext) is None:
                if ext == CSV:
                    fields_terminated_by = ','
                elif ext == TSV:
                    fields_terminated_by = '\\t'
                else:
                    fields_terminated_by = None
                self.fileformats[ext] = FileFormat(fields_terminated_by=fields_terminated_by)
