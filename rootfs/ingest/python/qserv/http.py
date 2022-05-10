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
User-friendly client library for Qserv replication service.

@author  Hsin Fang Chiang, Rubin Observatory
@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import getpass
import json
import logging
import os
from urllib.error import HTTPError
import urllib.parse

# ----------------------------
# Imports for other modules --
# ----------------------------
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from .exception import IngestError, ReplicationControllerError
from . import util

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
AUTH_PATH = "~/.lsst/qserv"

_LOG = logging.getLogger(__name__)


def authorize():
    try:
        with open(os.path.expanduser(AUTH_PATH), 'r') as f:
            authKey = f.read().strip()
    except IOError:
        _LOG.debug("Cannot find %s", AUTH_PATH)
        authKey = getpass.getpass()
    return authKey


def download_file(url: str, dest: str) -> None:
    response = requests.get(url, stream=True)
    text_file = open(dest, "wb")
    for chunk in response.iter_content(chunk_size=1024):
        text_file.write(chunk)
    text_file.close()


def file_exists(url: str) -> bool:
    """
    Check if a file exists on a remote HTTP server
    """
    response = requests.head(url)
    return (response.status_code == 200)


def json_get(base_url, filename):
    """
    Load json file at a given URL
    """
    str_url = urllib.parse.urljoin(util.trailing_slash(base_url), filename)
    url = urllib.parse.urlsplit(str_url, scheme="file")
    if url.scheme in ["http", "https"]:
        r = requests.get(str_url)
        return r.json()
    elif url.scheme == "file":
        with open(url.path, "r") as f:
            return json.load(f)
    else:
        raise IngestError("Unsupported URI scheme for ", url)


def _get_retry_object(retries=5, backoff_factor=0.2) -> Retry:
    """Create an instance of :obj:`urllib3.util.Retry`.

    With default arguments (5 retries with 0.2 backoff factor), urllib3 will sleep
    for 0.2, 0.4, 0.8, 1.6, 3.2 seconds between attempts.
    """

    # See https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/#retry-on-failure
    return Retry(
        total=retries,
        read=retries,
        connect=retries,
        method_whitelist=["GET"],
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
    )


class Http():
    """Manage http connections
    """

    def __init__(self):
        """ Set http connections retry/timeout errors
        """
        adapter = HTTPAdapter(max_retries=_get_retry_object())
        # Session is only used for the GET method
        self.http = requests.Session()
        self.http.mount("https://", adapter)
        self.http.mount("http://", adapter)

    def get(self, url, payload=dict(), auth=True, timeout=None) -> dict:
        if auth is True:
            authKey = authorize()
            payload["auth_key"] = authKey
        r = self.http.get(url, json=payload, timeout=timeout)
        if (r.status_code != 200):
            raise HTTPError('HTTP %s Error for url (GET): %s' % (r.status_code, url), response=r)
        responseJson = r.json()
        if not responseJson['success']:
            _LOG.critical("%s %s", url, responseJson['error'])
            raise ReplicationControllerError(
                'Error in JSON response (GET)', url,
                responseJson["error"])
        _LOG.info("GET: success")
        return responseJson

    def post(self, url, payload, auth=True, timeout=None) -> dict():
        if auth is True:
            authKey = authorize()
            payload["auth_key"] = authKey
        try:
            r = requests.post(url, json=payload, timeout=timeout)
        except (requests.exceptions.RequestException, ConnectionResetError) as e:
            _LOG.critical("Error when sending POST request to url %s", url)
            e.args = (f"POST request to url {url} with payload {payload} failed", *e.args)
            raise e
        if (r.status_code != 200):
            raise HTTPError('HTTP %s Error for url (POST): %s' % (r.status_code, url), response=r)
        responseJson = r.json()
        _LOG.debug("POST %s: success", url)
        return responseJson

    def put(self, url, payload=None, timeout=None) -> dict():
        authKey = authorize()
        if not payload:
            payload = {}
        payload["auth_key"] = authKey
        r = requests.put(url, json=payload, timeout=timeout)
        if (r.status_code != 200):
            raise HTTPError('HTTP %s Error for url (PUT): %s' % (r.status_code, url), response=r)
        responseJson = r.json()
        if not responseJson['success']:
            _LOG.critical("%s %s", url, responseJson['error'])
            raise ReplicationControllerError(
                'Error in JSON response (PUT)', url,
                responseJson["error"])
        _LOG.info("PUT: success")
        return responseJson

    def delete(self, url, timeout=None):
        authKey = authorize()
        r = requests.delete(url, json={"auth_key": authKey}, timeout=timeout)
        if (r.status_code != 200):
            raise HTTPError('HTTP %s Error for url (DELETE): %s' % (r.status_code, url), response=r)
        responseJson = r.json()
        if not responseJson['success']:
            _LOG.critical("%s %s", url, responseJson['error'])
            raise ReplicationControllerError(
                'Error in JSON response (DELETE)', url,
                responseJson["error"])
        _LOG.info("DELETE: success")
        return responseJson
