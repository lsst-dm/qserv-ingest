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


"""User-friendly client library for Qserv replication service.

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
import urllib.parse
from typing import Any, Dict, Optional

# ----------------------------
# Imports for other modules --
# ----------------------------
import requests
from requests.adapters import HTTPAdapter
from retry import retry
from urllib3.util import Retry

from . import util, version
from .exception import IngestError, ReplicationControllerError

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
DEFAULT_AUTH_PATH = "~/.lsst/qserv"
TIMEOUT_SHORT_SEC = 300
TIMEOUT_LONG_SEC = 600

_MAX_RETRY_ATTEMPTS = 3

_LOG = logging.getLogger(__name__)


def download_file(url: str, dest: str) -> None:
    response = requests.get(url, stream=True)
    text_file = open(dest, "wb")
    for chunk in response.iter_content(chunk_size=1024):
        text_file.write(chunk)
    text_file.close()


def file_exists(url: str) -> bool:
    """Check if a file exists on a remote HTTP server."""
    response = requests.head(url)
    return response.status_code == 200


def json_load(base_url: str, filename: str) -> Dict[Any, Any]:
    """Load a JSON file located at a given URL.

    Parameters
    ----------
    base_url: `str`
        JSON file location
    filename: `str`
        JSON file name

    Returns
    -------
    json_data: `dict`
        JSON data represented as a dictionary

    Raises
    ------
    IngestError:
        Raise is URI scheme is not in http://, https://, file://

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


def _get_retry_object(retries: int = 5, backoff_factor: float = 0.2) -> Retry:
    """Create an instance of :obj:`urllib3.util.Retry`.

    With default arguments (5 retries with 0.2 backoff factor), urllib3 will
    sleep for 0.2, 0.4, 0.8, 1.6, 3.2 seconds between attempts.

    """

    # See
    # https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/#retry-on-failure
    return Retry(
        total=retries,
        read=retries,
        connect=retries,
        allowed_methods=["GET"],
        backoff_factor=backoff_factor,
        status_forcelist=[429, 500, 502, 503, 504],
    )


class Http:
    """Manage http(s) connections
    designed to connect to Qserv Replication Controller"""

    def __init__(self, auth_path: Optional[str] = None) -> None:
        """Set http connections retry/timeout errors."""
        adapter = HTTPAdapter(max_retries=_get_retry_object())
        # Session is only used for the GET method
        self.http = requests.Session()
        self.http.mount("https://", adapter)
        self.http.mount("http://", adapter)
        self.authKey = self._authenticate(auth_path)

    def is_reachable(self, url: str) -> bool:
        """Check if a given http URL is reachable through the network."""
        try:
            self.http.head(url)
        except requests.exceptions.ConnectionError as e:
            _LOG.warning("Unable to connect to url %s, error: %s", url, e)
            return False
        return True

    def _authenticate(self, auth_path: Optional[str]) -> str:
        if not auth_path:
            auth_path = DEFAULT_AUTH_PATH
        try:
            with open(os.path.expanduser(auth_path), "r") as f:
                authKey = f.read().strip()
        except IOError:
            _LOG.warning("Cannot find %s", auth_path)
            authKey = getpass.getpass()
        return authKey

    def get(
        self, url: str, payload: Dict[str, Any] = dict(), auth: bool = True, timeout: Optional[int] = None
    ) -> Dict:
        """Send a GET query to replication controller/worker http(s) URL

        Parameters
        ----------
        url : `str`
            Http(s) URL
        payload : `dict` [`str`, `Any`], optional
            JSON payload, Defaults to dict().
        auth : `bool`, optional
            Perform HTTP authentication. Defaults to True.
        timeout : `int` optional
            Query time-out. Defaults to None.

        Raises
        ------
        ReplicationControllerError
            Raised if JSON response contain an error code

        Returns
        -------
        response_json : `dict`
            JSON response

        """
        if auth is True:
            payload["auth_key"] = self.authKey
        params = {"version": version.REPL_SERVICE_VERSION}
        r = self.http.get(url, params=params, json=payload, timeout=timeout)
        r.raise_for_status()
        response_json = r.json()
        if not response_json["success"]:
            _LOG.critical("%s %s", url, response_json["error"])
            raise ReplicationControllerError("Error in JSON response (GET)", url, response_json["error"])
        _LOG.debug("GET: success")
        return response_json

    def post(self, url: str, payload: Dict[str, Any] = None, auth: bool = True, timeout: int = None) -> Dict:
        """Send a POST query to an http(s) URL.

        Parameters
        ----------
        url : `str`
            Http(s) URL
        payload : `Dict[str, Any]`, optional
            JSON payload, Defaults to None.
        auth : `bool`, optional
            Perform HTTP authentication. Defaults to True.
        timeout : `int`, optional
            Query time-out. Defaults to None.

        Raises
        ------
        ReplicationControllerError
            Raised if JSON response contain an error code

        Returns
        -------
        response_json : `dict`
            JSON response

        """
        if payload is None:
            payload = dict()
        if auth is True:
            payload["auth_key"] = self.authKey
        try:
            r = requests.post(url, json=payload, timeout=timeout)
        except (requests.exceptions.RequestException, ConnectionResetError) as e:
            _LOG.critical("Error when sending POST request to url %s", url)
            e.args = (
                f"POST request to url {url} with payload {payload} failed",
                *e.args,
            )
            raise e
        r.raise_for_status()
        response_json = r.json()
        _LOG.debug("POST %s: success", url)
        return response_json

    @retry(requests.exceptions.Timeout, delay=5, tries=_MAX_RETRY_ATTEMPTS)
    def post_retry(self, url: str, payload: Dict[str, Any] = None, auth: bool = True) -> Dict:
        """Send a POST query to an http(s) URL and retry on time-out error.

        Parameters
        ----------
        url : `str`
            Http(s) URL
        payload : `Dict[ str, Any ]`, optional
            JSON payload, Defaults to None.
        auth : `bool`, optional
            Perform HTTP authentication. Defaults to True.

        Returns:
        response_json : `dict`
            JSON response
        """
        if payload is None:
            payload = dict()
        return self.post(url, payload, auth, TIMEOUT_SHORT_SEC)

    def put(self, url: str, payload: Dict[str, Any] = None, timeout: int = None) -> Dict:
        """Send a PUT query to an http(s) URL.

        Parameters
        ----------
        url : str
            Http(s) URL
        payload : Dict[str, Any], optional
            JSON payload, by default None
        timeout : int, optional
            Time-out for query, by default None

        Returns
        -------
        response_json : `dict`
            JSON response

        Raises
        ------
        ReplicationControllerError
            Raised if JSON response contain an error code
        """
        if payload is None:
            payload = dict()
            payload["version"] = version.REPL_SERVICE_VERSION
        payload["auth_key"] = self.authKey
        r = requests.put(url, json=payload, timeout=timeout)
        r.raise_for_status()
        response_json = r.json()
        if not response_json["success"]:
            _LOG.critical("%s %s", url, response_json["error"])
            raise ReplicationControllerError("Error in JSON response (PUT)", url, response_json["error"])
        _LOG.debug("PUT: success")
        return response_json

    def delete(self, url: str, timeout: int = None) -> Dict:
        """Send a DELETE query to an http(s) URL.

        Parameters
        ----------
        url : `str`
            Http(s) URL
        timeout : `int`, optional
            Time-out for query, by default None

        Returns
        -------
        response_json : `dict`
            JSON response

        Raises
        ------
        ReplicationControllerError
            Raised if JSON response contain an error code
        """
        json = {"version": version.REPL_SERVICE_VERSION, "auth_key": self.authKey}
        r = requests.delete(url, json=json, timeout=timeout)
        r.raise_for_status()
        response_json = r.json()
        if not response_json["success"]:
            _LOG.critical("%s %s", url, response_json["error"])
            raise ReplicationControllerError("Error in JSON response (DELETE)", url, response_json["error"])
        _LOG.debug("DELETE: success")
        return response_json
