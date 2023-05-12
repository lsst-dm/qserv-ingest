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

"""Unit tests for http.py.

@author  Fabrice Jammes, IN2P3

"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import argparse
import logging
import os

import pytest

# ----------------------------
# Imports for other modules --
# ----------------------------
import requests
from requests import HTTPError

from . import http, util, version

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

_LOG = logging.getLogger(__name__)


def test_file_exists() -> None:
    """Check if a file exists on a remote HTTP server."""
    assert http.file_exists("https://www.k8s-school.fr/team/index.html")
    assert not http.file_exists("https://www.k8s-school.fr/team/false.html")


def test_json_get() -> None:
    data = http.json_load(util.DATADIR, "servers.json")
    assert data["http_servers"][0] == "https://server1"
    assert data["http_servers"][2] == "https://server3"


def test_errorcode() -> None:
    """Check behaviour for error 404."""
    _http = http.Http()
    with pytest.raises(HTTPError) as e:
        url = "https://www.in2p3.cnrs.fr/notfound"
        _http.get(url=url, payload={}, auth=False)
    assert (
        e.value.args[0]
        == f"404 Client Error: Not Found for url: {url}?version={version.REPL_SERVICE_VERSION}"
    )


def test_retry() -> None:
    """Check if a retry occurs for a non-existing DNS entry.

    This might occurs if k8s DNS fails intermittently.

    """
    _http = http.Http()
    with pytest.raises(requests.ConnectionError):
        _http.get(url="http://server.not-exists", payload={}, auth=False)


def test_retry_post() -> None:
    """Check if an error occurs for a non-existing DNS entry."""
    _http = http.Http()
    with pytest.raises(requests.ConnectionError):
        _http.post_retry(url="http://server.not-exists", payload={})


def test_config() -> None:
    """Test configuration propagation from configuration file
    to http service"""
    parser = argparse.ArgumentParser(description="Test util module")
    util.add_default_arguments(parser)

    configfile = os.path.join(util.DATADIR, "dp02", "ingest.finetuned.yaml")
    args = parser.parse_args(["--config", configfile])

    _http = http.Http(args.config.http_read_timeout, args.config.http_write_timeout)

    assert _http.timeout_read_sec == 10
    assert _http.timeout_write_sec == 1800
