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
import logging
import os
import posixpath
import subprocess
import urllib.parse

# ----------------------------
# Imports for other modules --
# ----------------------------
import requests
from .queue import QueueManager
from .util import download_file

TMP_DIR = "/tmp"

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


def get_chunk_location(base_url, chunk, database, transaction_id):
    url = urllib.parse.urljoin(base_url, "ingest/chunk")
    payload = {"chunk": chunk,
               "database": database,
               "transaction_id": transaction_id}
    responseJson = post(url, payload)

    # Get location host and port
    host = responseJson["location"]["host"]
    port = responseJson["location"]["port"]
    _LOG.info("Location for chunk %d: %s %d" % (chunk, host, port))

    return (host, port)


def _ingest_chunk(host, port, transaction_id, chunk_file, table):

    cmd = ['qserv-replica-file-ingest', '--debug', '--verbose', 'FILE',
           host, str(port), str(transaction_id), table, "P", chunk_file]
    _LOG.debug("Launch unix process %s", cmd)

    result = subprocess.run(cmd,
                            capture_output=True,
                            universal_newlines=True,
                            check=True)
    _LOG.debug("stdout %s", result.stdout)
    _LOG.debug("stderr %s", result.stderr)
    return True


def ingest_task(base_url, connection, data_url):
    """Get a chunk from a queue server, load it inside Qserv, during a super-transation
        Returns
        -------
        Integer number: 0 if no chunk to load, 1 if chunk was loaded successfully
    """
    queue_manager = QueueManager(connection, data_url)

    _LOG.debug("Starting an ingest task: url: %s", base_url)

    chunk_info = queue_manager.lock_chunk()
    if not chunk_info:
        return 0

    (database, chunk_id, chunk_base_url, table) = chunk_info
    chunk_file = None
    transaction_id = None
    success = False
    try:
        transaction_id = start_transaction(base_url, database)
        (host, port) = get_chunk_location(base_url,
                                          chunk_id,
                                          database,
                                          transaction_id)
        chunk_file = _download_chunk(chunk_base_url, chunk_id, "chunk_{}.txt")
        _ingest_chunk(host, port, transaction_id, chunk_file, table)
        chunk_file = _download_chunk(
            chunk_base_url, chunk_id, "chunk_{}_overlap.txt")
        success = _ingest_chunk(host, port, transaction_id, chunk_file, table)
    except Exception as e:
        _LOG.critical('Error in ingest task for chunk %s: %s', chunk_info, e)
        raise(e)
    finally:
        if transaction_id:
            close_transaction(base_url, database, transaction_id, success)

    # TODO release chunk ni queue if process crash so that it can be locked by an other pod?
    # ingest successful
    queue_manager.delete_chunk()
    if chunk_file and os.path.isfile(chunk_file):
        os.remove(chunk_file)

    return 1


def _download_chunk(base_url, chunk_id, file_format):
    chunk_filename = file_format.format(chunk_id)
    abs_filename = download_file(base_url, chunk_filename)
    return abs_filename


def put(url):
    authKey = authorize()
    r = requests.put(url, json={"auth_key": authKey})
    if (r.status_code != 200):
        raise Exception(
            'Error in replication controller HTTP response (PUT)', url,
            r.status_code)
    responseJson = r.json()
    if not responseJson['success']:
        _LOG.critical("%s %s", url, responseJson['error'])
        raise Exception(
            'Error in replication controller JSON response (PUT)', url,
            responseJson["error"])
    _LOG.debug(responseJson)
    _LOG.info("success")


def post(url, payload):
    authKey = authorize()
    payload["auth_key"] = authKey
    _LOG.debug(payload)
    r = requests.post(url, json=payload)
    if (r.status_code != 200):
        raise Exception(
            'Error in replication controller HTTP response (POST)', url,
            r.status_code)
    responseJson = r.json()
    if not responseJson["success"]:
        _LOG.critical(responseJson["error"])
        raise Exception(
            'Error in replication controller response (POST)', url,
            responseJson["error"])
    _LOG.debug(responseJson)
    _LOG.debug("success")

    return responseJson


def start_transaction(base_url, database):
    url = urllib.parse.urljoin(base_url, "ingest/trans")
    payload = {"database": database}
    responseJson = post(url, payload)

    # For catching the super transaction ID
    # Want to print responseJson["databases"]["hsc_test_w_2020_14_00"]["transactions"]
    transaction_id = responseJson["databases"][database]["transactions"][0]["id"]
    _LOG.debug(f"transaction ID: {transaction_id}")

    return transaction_id


def close_transaction(base_url, database, transaction_id, success):
    tmp_url = posixpath.join("ingest/trans/", str(transaction_id))
    if success is True:
        tmp_url += "?abort=0&build-secondary-index=1"
    else:
        tmp_url += "?abort=1"
    url = urllib.parse.urljoin(base_url, tmp_url)
    responseJson = put(url)
    _LOG.debug("Close transaction (responseJson: %s)", responseJson)
