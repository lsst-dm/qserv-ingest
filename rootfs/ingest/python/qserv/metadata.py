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

"""Manage metadata related to input data.

@author  Fabrice Jammes, IN2P3

"""

import logging
import sys
import urllib.parse

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from collections.abc import Generator
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from qserv.loadbalancerurl import LoadBalancedURL, LoadBalancerAlgorithm

# ----------------------------
# Imports for other modules --
# ----------------------------
from . import version
from .http import json_load

CSV = "csv"
TSV = "tsv"
TXT = "txt"
EXT_LIST: List[str] = [CSV, TSV, TXT]
"""List of supported input data file extensions
"""

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_CHUNKS: str = "chunks"
_FILES: str = "files"
_METADATA_FILENAME: str = "metadata.json"
_MIN_SUPPORTED_VERSION = 12
_OVERLAPS: str = "overlaps"
_LOG = logging.getLogger(__name__)


@dataclass
class FileFormat:
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

    fields_enclosed_by: Optional[str] = None
    fields_escaped_by: Optional[str] = None
    fields_terminated_by: Optional[str] = None
    lines_terminated_by: Optional[str] = None


@dataclass
class TableContributionsSpec:
    """Contain contribution specification for a given table and for a given
    path.

    Store informations which will allow to retrieve contributions file. Each
    entry of the list is a tuple: (<path>, [chunk_ids], <is_overlap>, <table>)
    where [chunk_ids] is the list of the contribution files (XOR overlap)
    available at a given path for a given table and a given chunk.

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
    """Chunks ids for files for partitioned tables,
    empty for non-partitioned tables
    """

    chunks_overlap: List[int]
    """Chunks ids for overlap files for partioned tables,
    empty for non-partitioned tables
    """

    def get_contrib(self) -> Generator[Dict[str, Any], None, None]:
        """Generator for contribution specifications for a given table and a
        given path.

        Yields
        ------
        data: `Iterator[List[dict()]]`
            Iterator on each contribution specifications for a table

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
    """Contain table specifications.

    Parameters:
    -----------
    metadata_url : `str`
        url of metadata, used to access tables' json
        configuration files for R-I service
    table_meta : `Dict`
        metadata for a table

    """

    def __init__(self, metadata_url: str, table_meta: Dict):

        self.data: List[Any] = table_meta["data"]
        schema_file: str = table_meta["schema"]
        self.json_schema: Dict[Any, Any] = json_load(metadata_url, schema_file)
        self.name: str = self.json_schema["table"]
        self.database: str = self.json_schema["database"]
        self.is_partitioned: bool = self.json_schema["is_partitioned"] == 1
        self.is_director: bool = self._is_director()
        idx_files: List[str] = table_meta.get("indexes", [])
        self.json_indexes: List[Dict[str, Any]] = []
        for f in idx_files:
            self.json_indexes.append(json_load(metadata_url, f))

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
                    # chunk ids for overlaps might be different
                    # of regular chunk ids
                    if d.get(_OVERLAPS):
                        chunks_overlap = d[_OVERLAPS]
                    else:
                        chunks_overlap = d[_CHUNKS]
            else:
                files = d[_FILES]
            self.contrib_specs.append(
                TableContributionsSpec(path, self.database, self.name, files, chunks, chunks_overlap)
            )

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

    def __init__(
        self,
        metadata_url: str,
        datapath: str,
        loadbalancers: List[str] = [],
        auto_build_secondary_index: Optional[int] = None,
    ):
        """Retrieve and store metadata located at 'path' and describing:

             - database
             - tables
             - contribution files

           Support for file:// and http(s):// protocols

        Parameters
        ----------
        metadata_url : `str`
            Path to metadata
        """
        self._database: str
        self._json_db: Dict[str, Any] = {}
        self.fileformats: Dict[str, FileFormat] = {}
        self._tableSpecs: List[TableSpec]

        lbAlgo = LoadBalancerAlgorithm(loadbalancers)
        self.lb_url = LoadBalancedURL(datapath, lbAlgo)

        self.metadata_url = metadata_url
        # FIXME not all steps require to load full metadata.json
        self.metadata = json_load(self.metadata_url, _METADATA_FILENAME)
        self._check_version()

        filename = self.metadata["database"]
        self._json_db = json_load(self.metadata_url, filename)
        # Override metadata value for parameter "auto_build_secondary_index"
        # with ingest.yaml parameter value
        if auto_build_secondary_index is not None:
            self._json_db["auto_build_secondary_index"] = auto_build_secondary_index
        self._database = self._json_db["database"]
        self.family = "layout_{}_{}".format(self._json_db["num_stripes"], self._json_db["num_sub_stripes"])
        self._init_tables()
        self._init_fileformats()

    @property
    def database(self) -> str:
        """Getter for the database property

        Returns
        -------
        database: `str`
            List of tables specification available in metadata file
        """
        return self._database

    @property
    def charset_name(self) -> str:
        """Getter for the charset_name property

        Returns
        -------
        charset_name: `str`
            charset_name used for all the contribution files,
            use replication service default if not set
        """
        return self.metadata.get("charset_name", "")

    @property
    def tableSpecs(self) -> List[TableSpec]:
        """Getter for the tableSpecs property

        Returns
        -------
        tableSpecs: List[TableSpec]
            List of table specifications available in metadata file
        """
        return self._tableSpecs

    @property
    def json_db(self) -> Dict[str, Any]:
        """Getter for the json_db property

        Returns
        -------
        json_db: Dict[str, Any]
            Database configuration issued from json configuration
        """
        return self._json_db

    def _check_version(self) -> None:
        """Check metadata file version
        and exit if its value is not supported"""
        fileversion = None
        if "version" in self.metadata:
            fileversion = self.metadata["version"]

        if fileversion is None or not (_MIN_SUPPORTED_VERSION <= fileversion <= version.REPL_SERVICE_VERSION):
            _LOG.critical(
                "The metadata file (%s) version is not in the range supported by qserv-ingest "
                "(is %s, expected between %s and %s)",
                self.metadata_url,
                fileversion,
                _MIN_SUPPORTED_VERSION,
                version.REPL_SERVICE_VERSION,
            )
            sys.exit(1)
        _LOG.info("Metadata file version: %s", version.REPL_SERVICE_VERSION)

    @property
    def table_contribs_spec(self) -> Generator[TableContributionsSpec, None, None]:
        """Generator for contribution specifications for the whole database.

        Retrieve information about input contribution files
        in order to insert them inside the contribution files queue

        Yields
        ------
        data: `Iterator[List[dict()]]`
            Iterator on each contribution specifications for a database

        """
        for table in self._tableSpecs:
            yield from table.contrib_specs

    def file_url(self, path: str) -> str:
        """Return the url of a file located on the metadata data server."""
        return urllib.parse.urljoin(self.metadata_url, path)

    @property
    def table_names(self) -> List[str]:
        """Get list of table names available in metadata file

        Returns
        -------
        List[str]
            List of table names available in metadata file
        """
        table_names = []
        for t in self._tableSpecs:
            table_names.append(t.name)
        return table_names

    @property
    def json_indexes(self) -> List[Dict[str, Any]]:
        json_indexes: List[Dict] = []
        for tbl in self._tableSpecs:
            json_indexes.extend(tbl.json_indexes)
        return json_indexes

    @property
    def ordered_tables_json(self) -> List[Dict[Any, Any]]:
        """Retrieve json schema for each tables of a given database in order to
        register them inside the replication service.

        Returns
        -------
        l: List[str]
            a list of json schemas in the R-I system format,
            one for each table, director tables are at the beginning
            of the list

        """
        schema_files = []
        for t in self._tableSpecs:
            schema_files.append(t.json_schema)
        return schema_files

    def _init_tables(self) -> None:
        self._tableSpecs = []
        self._has_extra_overlaps = False
        for table_meta in self.metadata["tables"]:
            table = TableSpec(self.metadata_url, table_meta)
            if table.is_director:
                self._tableSpecs.insert(0, table)
            else:
                self._tableSpecs.append(table)

    def _init_fileformats(self) -> None:
        format = self.metadata.get("formats")
        if format:
            for ext in EXT_LIST:
                format_spec = format.get(ext)
                if format_spec:
                    self.fileformats[ext] = FileFormat(**format_spec)

        for ext in EXT_LIST:
            if self.fileformats.get(ext) is None:
                if ext == CSV:
                    fields_terminated_by = ","
                elif ext == TSV:
                    fields_terminated_by = "\\t"
                else:
                    fields_terminated_by = None
                self.fileformats[ext] = FileFormat(fields_terminated_by=fields_terminated_by)
