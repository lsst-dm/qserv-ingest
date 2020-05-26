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
import subprocess
import sys

# ----------------------------
# Imports for other modules --
# ----------------------------
import requests

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
    response = requests.post(url, json=payload)
    del payload["auth_key"]
    responseJson = response.json()
    if not responseJson["success"]:
        logging.critical(responseJson["error"])
        raise Exception('Error in replication controller response (POST)', url, responseJson["error"])
    logging.debug(responseJson)
    logging.debug("success")

    return responseJson

def start_transaction(base_url, database):
    url = format("%s/ingest/v1/trans",base_url)
    payload={"database":database}
    responseJson = post(url,payload)

    # For catching the super transaction ID
    # Want to print responseJson["databases"]["hsc_test_w_2020_14_00"]["transactions"]
    transaction_id = responseJson["databases"][database]["transactions"][0]["id"]
    logging.debug(f"transaction ID: {transaction_id}")
    
    return transaction_id

def stop_transaction(base_url, database, transaction_id):
    url = format("%s/ingest/v1/trans/%s?abort=0&build-secondary-index=1", base_url, transaction_id)
    responseJson = put(url)

def get_chunk_location(base_url, chunk, transaction_id):
    url = format("%s/%s",base_url,"/ingest/v1/trans")
    payload={"transaction_id":transaction_id,"chunk":chunk} 
    responseJson = post(url,payload)

    # For catching the location host and port
    if "location" in responseJson and "chunk" in payload:
        host = responseJson["location"]["host"]
        port = responseJson["location"]["port"]
        logging.info("%d %s %d" % (payload["chunk"], host, port))
    
    return (host, port)

def ingest_chunk(host, port, transaction_id, chunk_file):
    cmd= ['qserv-replica-file-ingest', '--debug', '--verbose', 'FILE', host, port, transaction_id, "position", "P", chunk_file]
    process = subprocess.run(cmd, 
                         stdout=subprocess.PIPE, 
                         universal_newlines=True)

def ingest_task(base_url, database, chunk, chunk_path):
    logging.debug("Starting an ingest task: url: %s, db: %s, chunk: %s", base_url, database, chunk)
    transaction_id = start_transaction(base_url, database)
    (host, port) = get_chunk_location(base_url, chunk, transaction_id)
    chunk_file = os.path.join(chunk_path, format("chunk_%s.txt", chunk))
    ingest_chunk(host, port, transaction_id, chunk_file)
    stop_transaction(base_url, database, transaction_id)

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

