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

# ----------------------------
# Imports for other modules --
# ----------------------------
import argparse
import logging
import os

from . import util

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------


_LOG = logging.getLogger(__name__)


def test_ingestconfig() -> None:
    """Check the function which compare files in two directories."""

    parser = argparse.ArgumentParser(description="Test util module")
    util.add_default_arguments(parser)

    configfile = os.path.join(util.DATADIR, "dp02", "ingest.yaml")
    args = parser.parse_args(["--config", configfile])

    _LOG.debug("Replication configuration: %s", args.config.ingestservice)

    assert args.config.ingestservice.cainfo == "/etc/pki/tls/certs/ca-bundle.crt"
    assert args.config.ingestservice.ssl_verifypeer == 1
    assert args.config.ingestservice.low_speed_limit == 60
    assert args.config.ingestservice.low_speed_time == 120

    configfile = os.path.join(util.DATADIR, "dp02", "ingest.finetuned.yaml")
    args = parser.parse_args(["--config", configfile])

    assert args.config.ingestservice.cainfo == "/etc/pki/tls/certs/ca-bundle.crt"
    assert args.config.ingestservice.ssl_verifypeer == 1
    assert args.config.ingestservice.low_speed_limit == 10
    assert args.config.ingestservice.low_speed_time == 3600
