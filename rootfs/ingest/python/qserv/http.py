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
from typing import Any, Dict, Tuple, Union

# ----------------------------
# Imports for other modules --
# ----------------------------
import requests
from qserv import jsonparser
from requests.adapters import HTTPAdapter
from retry import retry
from urllib3.util import Retry

from . import util, version
from .exception import IngestError, ReplicationControllerError

DEFAULT_AUTH_PATH = "~/.lsst/qserv"
DEFAULT_TIMEOUT_READ_SEC = 300.0
DEFAULT_TIMEOUT_WRITE_SEC = 600.0

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_DEFAULT_CONNECTION_TIMEOUT = 5.0
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

    def __init__(
        self,
        timeout_read_sec: float = DEFAULT_TIMEOUT_READ_SEC,
        timeout_write_sec: float = DEFAULT_TIMEOUT_WRITE_SEC,
        auth_path: str = DEFAULT_AUTH_PATH,
    ) -> None:
        """Set http connections retry/timeout errors."""
        self.auth_path = auth_path
        adapter = HTTPAdapter(max_retries=_get_retry_object())
        # Session is only used for the GET method
        self.http = requests.Session()
        self.http.mount("https://", adapter)
        self.http.mount("http://", adapter)
        self.authKey = self._authenticate()
        self.timeout_read_sec = timeout_read_sec
        self.timeout_write_sec = timeout_write_sec

    def is_reachable(self, url: str) -> bool:
        """Check if a given http URL is reachable through the network."""
        try:
            self.http.head(url)
        except requests.exceptions.ConnectionError as e:
            _LOG.warning("Unable to connect to url %s, error: %s", url, e)
            return False
        return True

    def _authenticate(self) -> str:
        try:
            with open(os.path.expanduser(self.auth_path), "r") as f:
                authKey = f.read().strip()
        except IOError:
            _LOG.warning("Cannot find %s", self.auth_path)
            authKey = getpass.getpass()
        return authKey

    def get(self, url: str, payload: Dict[str, Any] = dict(), auth: bool = True) -> Dict:
        """Send a GET query to replication controller/worker http(s) URL

        Parameters
        ----------
        url : `str`
            Http(s) URL
        payload : `dict` [`str`, `Any`], optional
            JSON payload, Defaults to dict().
        auth : `bool`, optional
            Perform HTTP authentication. Defaults to True.

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
        r = self.http.get(url, params=params, json=payload, timeout=self.timeout_read_sec)
        r.raise_for_status()
        response_json = r.json()
        jsonparser.raise_error(response_json)
        _LOG.debug("GET: success")
        return response_json

    def post(
        self, url: str, payload: Dict[str, Any] = None, auth: bool = True, no_readtimeout: bool = False
    ) -> Dict:
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
        # Set version if it does not exists
        payload["version"] = payload.get("version", version.REPL_SERVICE_VERSION)
        if auth is True:
            payload["auth_key"] = self.authKey
        timeouts: Union[float, Tuple[float, float], Tuple[float, None]]
        if no_readtimeout:
            timeouts = (_DEFAULT_CONNECTION_TIMEOUT, None)
        else:
            timeouts = (_DEFAULT_CONNECTION_TIMEOUT, self.timeout_write_sec)
        try:
            r = requests.post(url, json=payload, timeout=timeouts)
        except (requests.exceptions.RequestException, ConnectionResetError) as e:
            _LOG.critical("Error when sending POST request to url %s", url)
            e.args = (
                f"POST request to url {url} with payload {payload} failed",
                *e.args,
            )
            raise e
        r.raise_for_status()
        response_json = r.json()
        jsonparser.raise_error(response_json)
        _LOG.debug("POST %s: success", url)
        return response_json

    @retry(requests.exceptions.ConnectTimeout, delay=5, tries=_MAX_RETRY_ATTEMPTS)
    def post_retry(
        self, url: str, payload: Dict[str, Any] = None, auth: bool = True, no_readtimeout: bool = False
    ) -> Dict:
        """Send a POST query to an http(s) URL and retry on time-out error.

        Parameters
        ----------
        url : `str`
            Http(s) URL
        timeout : `int`
            Timeout in seconds
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
        return self.post(url, payload, auth, no_readtimeout)

    def put(self, url: str, payload: Dict[str, Any] = None, no_readtimeout: bool = True) -> Dict:
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

        # Set version if it does not exists
        payload["version"] = payload.get("version", version.REPL_SERVICE_VERSION)
        payload["auth_key"] = self.authKey

        timeouts: Union[float, Tuple[float, float], Tuple[float, None]]
        if no_readtimeout:
            timeouts = (_DEFAULT_CONNECTION_TIMEOUT, None)
        else:
            timeouts = (_DEFAULT_CONNECTION_TIMEOUT, self.timeout_write_sec)
        r = requests.put(url, json=payload, timeout=timeouts)
        r.raise_for_status()
        response_json = r.json()
        jsonparser.raise_error(response_json)
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


def get_fqdn(fqdns: str, port: int, scheme: str = "http") -> str:
    """Return fqdn of the first reachable scheme://fqdn:port entry.

    Parameters
    ----------
    fqdns: `str`
        comma-separated list of fqdns
    port: `int`
        url port to reach

    Returns
    -------
    fqdn : `str`
        First reachable host fqdn, empty string if not fqdn is reachable

    """
    http = Http()
    for fqdn in fqdns.split(","):
        url = f"{scheme}://{fqdn}:{port}"
        if http.is_reachable(url):
            return fqdn
    return ""
