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
@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------

import logging

# ----------------------------
# Imports for other modules --
# ----------------------------
import os

import yaml

from . import util
from .ingestconfig import IngestConfig

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

_LOG = logging.getLogger(__name__)


def test_ingestconfig_metadata_url() -> None:
    """Check support for metadata url parameter in configuration"""
    config_file = os.path.join(util.DATADIR, util.DP02, "ingest.yaml")
    with open(config_file, "r") as values:
        yaml_data = yaml.safe_load(values)

    config = IngestConfig(yaml_data)

    assert config.metadata_url == "https://raw.githubusercontent.com/rubin-in2p3/qserv-ingest-schema/main/"
