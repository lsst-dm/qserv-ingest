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
Tools used by ingest algorithm

@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------

# ----------------------------
# Imports for other modules --
# ----------------------------
from . import metadata
import logging
import os

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

_CWD = os.path.dirname(os.path.abspath(__file__))
_LOG = logging.getLogger(__name__)


def test_get_ordered_tables_json():
    data_url = os.path.join(_CWD, "testdata", "dp01_dc2_catalogs")
    contribution_metadata = metadata.ContributionMetadata(data_url)
    tables_json_data = contribution_metadata.get_ordered_tables_json()
    tables = []
    for json_data in tables_json_data:
        tables.append(json_data["table"])
    _LOG.info("Ordered list of tables %s", tables)

    assert tables == [
        "object",
        "position",
        "forced_photometry",
        "reference",
        "truth_match",
    ]


def test_get_contribution_file_specs_dp01() -> None:
    data_url = os.path.join(_CWD, "testdata", "dp01_dc2_catalogs")
    contribution_metadata = metadata.ContributionMetadata(data_url)
    contrib_count = 0
    contrib_director_count = 0
    contrib_director_overlap_count = 0
    contrib_director_chunk_count = 0
    for table_contrib_spec in contribution_metadata.get_table_contribs_spec():
        for contrib_spec in table_contrib_spec.get_contrib():
            contrib_count += 1
            contrib_spec["database"] = contribution_metadata.database
            if contrib_spec["table"] == "object":
                contrib_director_count += 1
                if contrib_spec["is_overlap"]:
                    contrib_director_overlap_count += 1
                else:
                    contrib_director_chunk_count += 1

    assert contrib_count == 13040
    assert contrib_director_overlap_count == 2197
    assert contrib_director_chunk_count == 2111


def test_get_contribution_file_specs_case01() -> None:
    data_url = os.path.join(_CWD, "testdata", "case01")
    contribution_metadata = metadata.ContributionMetadata(data_url)
    contrib_count = 0
    contrib_director_count = 0
    contrib_director_overlap_count = 0
    contrib_director_chunk_count = 0
    for table_contrib_spec in contribution_metadata.get_table_contribs_spec():
        for contrib_spec in table_contrib_spec.get_contrib():
            contrib_count += 1
            contrib_spec["database"] = contribution_metadata.database
            if contrib_spec["table"] == "Logs":
                assert contrib_spec["filepath"] == "/Logs.tsv"
            if contrib_spec["table"] == "Object":
                contrib_director_count += 1
                if contrib_spec["is_overlap"]:
                    contrib_director_overlap_count += 1
                else:
                    contrib_director_chunk_count += 1

    _LOG.info("contrib_director_chunk_count %s", contrib_director_chunk_count)
    _LOG.info("contrib_director_overlap_count %s", contrib_director_overlap_count)

    assert contrib_director_chunk_count == 13
    assert contrib_director_overlap_count == 10
    assert contrib_count == 37


def test_get_table_names() -> None:
    data_url = os.path.join(_CWD, "testdata", "dp01_dc2_catalogs")
    contribution_metadata = metadata.ContributionMetadata(data_url)
    table_names = contribution_metadata.get_tables_names()

    assert table_names == ["object", "position", "forced_photometry", "reference", "truth_match"]


def test_is_director() -> None:
    data_url = os.path.join(_CWD, "testdata", "dp02")
    contribution_metadata = metadata.ContributionMetadata(data_url)
    table_names = contribution_metadata.get_tables_names()
    _LOG.info("tables: %s", table_names)
    _LOG.info("table %s", contribution_metadata.tables[0].name)
    _LOG.info("table %s", contribution_metadata.tables[1].name)

    # case: "director_table":""
    idx = table_names.index("Source")
    tableSpec = contribution_metadata.tables[idx]
    assert tableSpec._is_director()

    #  case: no "director_table" field, and "is_partitioned": 1
    idx = table_names.index("DiaObject")
    tableSpec = contribution_metadata.tables[idx]
    assert tableSpec._is_director()

    #  case: parititioned non director table
    idx = table_names.index("DiaSource")
    tableSpec = contribution_metadata.tables[idx]
    assert not tableSpec._is_director()

    #  case: regular non director table
    idx = table_names.index("CcdVisit")
    tableSpec = contribution_metadata.tables[idx]
    assert not tableSpec._is_director()


def test_get_contribution_file_specs_dp02() -> None:
    data_url = os.path.join(_CWD, "testdata", "dp02")
    contribution_metadata = metadata.ContributionMetadata(data_url)
    contrib_count = 0
    contrib_director_count = 0
    contrib_source_count = 0
    contrib_director_overlap_count = 0
    contrib_director_chunk_count = 0
    for table_contrib_spec in contribution_metadata.get_table_contribs_spec():
        for contrib_spec in table_contrib_spec.get_contrib():
            contrib_count += 1
            contrib_spec["database"] = contribution_metadata.database
            if contrib_spec["table"] == "Object":
                contrib_director_count += 1
                if contrib_spec["is_overlap"]:
                    contrib_director_overlap_count += 1
                else:
                    contrib_director_chunk_count += 1
            if contrib_spec["table"] == "Source":
                contrib_source_count += 1

    assert contrib_director_chunk_count == 2469
    assert contrib_director_overlap_count == 2508
    assert contrib_source_count == 1661647
    assert contrib_count == 1692713

