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
import argparse
import getpass
import json
import logging
import os
import posixpath
import subprocess
import sys
import urllib.parse

# ----------------------------
# Imports for other modules --
# ----------------------------
import requests
import sqlalchemy
from sqlalchemy.engine.url import make_url
from .queue import *

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
AUTH_PATH = "~/.lsst/qserv"

def authorize():
    try:
        with open(os.path.expanduser(AUTH_PATH), 'r') as f:
            authKey = f.read().strip()
    except:
        logging.debug("Cannot find %s", AUTH_PATH)
        authKey = getpass.getpass()
    return authKey

# def get_chunk():
#     mariadb_connection = mariadb.connect(user='python_user', password='some_pass', database='employees')
#     cursor = mariadb_connection.cursor()
#     cursor.execute("SELECT first_name,last_name FROM employees WHERE first_name=%s", "toto")

def get_chunk_location(base_url, chunk, database, transaction_id):
    url = urllib.parse.urljoin(base_url,"ingest/v1/chunk")
    payload={"chunk":chunk,
             "database":database,
             "transaction_id":transaction_id}
    responseJson = post(url,payload)

    # Get location host and port
    host = responseJson["location"]["host"]
    port = responseJson["location"]["port"]
    logging.info("Location for chunk %d: %s %d" % (chunk, host, port))

    return (host, port)

def ingest_chunk(host, port, transaction_id, chunk_file):
    cmd= ['qserv-replica-file-ingest', '--debug', '--verbose', 'FILE', host, str(port), str(transaction_id), "position", "P", chunk_file]
    process = subprocess.run(cmd,
                         stdout=subprocess.PIPE,
                         universal_newlines=True)

    # TODO get error code

def ingest_task(base_url, database, connection):
    db_url = make_url(connection)
    engine = sqlalchemy.create_engine(db_url)
    queue_manager = QueueManager(engine)
    logging.debug("Starting an ingest task: url: %s, db: %s", base_url, database)

    transaction_id = start_transaction(base_url, database)
    try:
        (host, port) = get_chunk_location(base_url, chunk, database, transaction_id)
        chunk_filename = "chunk_{}.txt".format(chunk)
        chunk_file = os.path.join(chunk_path, chunk_filename)
        ingest_chunk(host, port, transaction_id, chunk_file)
        stop_transaction(base_url, database, transaction_id)
    except Exception as e:
        # TODO abort transaction
        raise Exception('Error in ingest task: %s', e)

def put(url):
    authKey = authorize()
    response = requests.put(url, json={"auth_key": authKey})
    responseJson = response.json()
    if not responseJson['success']:
        logging.critical("%s %s", url, responseJson['error'])
        raise Exception('Error in replication controller response (PUT)', url, responseJson["error"])
    logging.debug(responseJson)
    logging.info("success")

def post(url, payload):
    authKey = authorize()
    payload["auth_key"] = authKey
    logging.debug(payload)
    response = requests.post(url, json=payload)
    responseJson = response.json()
    if not responseJson["success"]:
        logging.critical(responseJson["error"])
        raise Exception('Error in replication controller response (POST)', url, responseJson["error"])
    logging.debug(responseJson)
    logging.debug("success")

    return responseJson

def start_transaction(base_url, database):
    url = urllib.parse.urljoin(base_url,"ingest/v1/trans")
    payload={"database":database}
    responseJson = post(url,payload)

    # For catching the super transaction ID
    # Want to print responseJson["databases"]["hsc_test_w_2020_14_00"]["transactions"]
    transaction_id = responseJson["databases"][database]["transactions"][0]["id"]
    logging.debug(f"transaction ID: {transaction_id}")

    return transaction_id

def stop_transaction(base_url, database, transaction_id):
    tmp_url = posixpath.join("ingest/v1/trans/",str(transaction_id))
    tmp_url += "?abort=0&build-secondary-index=1"
    url = urllib.parse.urljoin(base_url, tmp_url)
    responseJson = put(url)


class DataAction(argparse.Action):
    """argparse action to attempt casting the values to floats and put into a dict"""

    def __call__(self, parser, namespace, values, option_string):
        d = dict()
        for item in values:
            k, v = item.split("=")
            try:
                v = float(v)
            except ValueError:
                pass
            finally:
                d[k] = v
        setattr(namespace, self.dest, d)

class JsonAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        with open(values, 'r') as f:
            x = json.load(f)
        setattr(namespace, self.dest, x)
