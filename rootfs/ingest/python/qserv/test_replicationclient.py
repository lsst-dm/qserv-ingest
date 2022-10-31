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

"""Tests for util modules.

@author  Fabrice Jammes, IN2P3

"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import logging
import os
import tempfile

import pytest

# ----------------------------
# Imports for other modules --
# ----------------------------
import requests

from . import http, metadata, replicationclient, util

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

_LOG = logging.getLogger(__name__)


def test_database_register_tables() -> None:
    """Test function database_register_tables.

    TODO add mock replication server for testing database_register_tables()
    method

    """

    data_url = os.path.join(util.DATADIR, "dp01_dc2_catalogs")
    contribution_metadata = metadata.ContributionMetadata(data_url)
    tables_json_data = contribution_metadata.ordered_tables_json

    _, auth_path = tempfile.mkstemp()

    _LOG.debug("Credentials: %s", http.DEFAULT_AUTH_PATH)
    with pytest.raises(requests.exceptions.ConnectionError) as e:
        repl_client = replicationclient.ReplicationClient("http://no_url/", auth_path)
        # TODO add mock replication server for method below
        repl_client.database_register_tables(tables_json_data, None)
    _LOG.error("Expected error: %s", e)
