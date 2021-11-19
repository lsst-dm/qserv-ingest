#!/usr/bin/env python

# LSST Data Management System
# Copyright 2014-2015 AURA/LSST.
#
# This product includes software developed by the
# LSST Project (http://www.lsst.org/).
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
# You should have received a copy of the LSST License Statement and
# the GNU General Public License along with this program.  If not,
# see <http://www.lsstcorp.org/LegalNotices/>.

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
import urllib.parse

# ----------------------------
# Imports for other modules --
# ----------------------------
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
from . import util

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
AUTH_PATH = "~/.lsst/qserv"
DEFAULT_TIMEOUT=11
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
    response = requests.get(url, stream = True)
    text_file = open(dest,"wb")
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
        raise Exception("Unsupported URI scheme for ", url)

class TimeoutHTTPAdapter(HTTPAdapter):
    """ Manage http connection time-out
        See https://findwork.dev/blog/advanced-usage-python-requests-timeouts-retries-hooks/
    """
    def __init__(self, *args, **kwargs):
        self.timeout = DEFAULT_TIMEOUT
        if "timeout" in kwargs:
            self.timeout = kwargs["timeout"]
            del kwargs["timeout"]
        super().__init__(*args, **kwargs)

    def send(self, request, **kwargs):
        timeout = kwargs.get("timeout")
        if timeout is None:
            kwargs["timeout"] = self.timeout
        return super().send(request, **kwargs)

class Http():
    """Manage http connections
    """

    def __init__(self):
        """ Set http connections retry/timeout errors
        """

        retry_strategy = Retry(
            backoff_factor=1,
            total=10,
            status_forcelist=[429, 500, 502, 503, 504],
            method_whitelist=["HEAD", "GET", "PUT", "DELETE", "OPTIONS", "TRACE"]
        )

        adapter = TimeoutHTTPAdapter(max_retries=retry_strategy)
        self.http = requests.Session()
        self.http.mount("https://", adapter)
        self.http.mount("http://", adapter)

    def get(self, url, payload=dict(), auth=True) -> dict:
        if auth == True:
            authKey = authorize()
            payload["auth_key"] = authKey
        r = requests.get(url, json=payload)
        if (r.status_code != 200):
            raise Exception(
                'Error in HTTP response (GET)', url,
                r.status_code)
        responseJson = r.json()
        if not responseJson['success']:
            _LOG.critical("%s %s", url, responseJson['error'])
            raise Exception(
                'Error in JSON response (GET)', url,
                responseJson["error"])
        _LOG.info("GET: success")
        return responseJson

    def post(self, url, payload, auth=True):
        if auth == True:
            authKey = authorize()
            payload["auth_key"] = authKey
        try:
            r = requests.post(url, json=payload)
        except (requests.exceptions.RequestException, ConnectionResetError) as e:
            _LOG.critical("Error when sending POST request to url %s", url)
            e.args = (f"POST request to url {url} with payload {payload} failed", *e.args)
            raise e
        if (r.status_code != 200):
            raise Exception(
                'Error in HTTP response (POST)', url,
                r.status_code)
        responseJson = r.json()
        _LOG.debug("POST %s: success", url)
        return responseJson

    def put(self, url, payload=None):
        authKey = authorize()
        if not payload:
            payload = {}
        payload["auth_key"] = authKey
        r = requests.put(url, json=payload)
        if (r.status_code != 200):
            raise Exception(
                'Error in HTTP response (PUT)', url,
                r.status_code)
        responseJson = r.json()
        if not responseJson['success']:
            _LOG.critical("%s %s", url, responseJson['error'])
            raise Exception(
                'Error in JSON response (PUT)', url,
                responseJson["error"])
        _LOG.info("PUT: success")
        return responseJson

    def delete(self, url):
        authKey = authorize()
        r = requests.delete(url, json={"auth_key": authKey})
        if (r.status_code != 200):
            raise Exception(
                'Error in HTTP response (DELETE)', url,
                r.status_code)
        responseJson = r.json()
        if not responseJson['success']:
            _LOG.critical("%s %s", url, responseJson['error'])
            raise Exception(
                'Error in JSON response (DELETE)', url,
                responseJson["error"])
        _LOG.info("DELETE: success")
        return responseJson
