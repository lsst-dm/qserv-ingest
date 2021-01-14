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
Tools used by ingest algorithm

@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import argparse
import json
import logging
import os
import urllib.parse
import yaml

# ----------------------------
# Imports for other modules --
# ----------------------------
import requests

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

TMP_DIR = "/tmp"
_LOG = logging.getLogger(__name__)

def http_file_exists(base_url, filename):
    """Check if a file exists on a remote HTTP server
    """
    str_url = urllib.parse.urljoin(base_url, filename)
    response = requests.head(str_url)
    return (response.status_code == 200)


def json_get(base_url, filename):
    """Load json file at a given URL
    """
    str_url = urllib.parse.urljoin(base_url, filename)
    url = urllib.parse.urlsplit(str_url, scheme="file")
    if url.scheme in ["http", "https"]:
        r = requests.get(str_url)
        return r.json()
    elif url.scheme == "file":
        with open(url.path, "r") as f:
            return json.load(f)
    else:
        raise Exception("Unsupported URI scheme for ", url)


def trailing_slash(url):
    if not url.endswith('/'):
        url += '/'
    return url


class BaseUrlAction(argparse.Action):
    def __call__(self, parser, namespace, values, option_string):
        x = trailing_slash(values)
        setattr(namespace, self.dest, x)


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


class FelisAction(argparse.Action):
    """argparse action to read a felis file into namespace"""

    def __call__(self, parser, namespace, values, option_string):
        with open(values, "r") as f:
            tables = yaml.safe_load(f)["tables"]
        schemas = dict()
        for table in tables:
            tableName = table["name"]
            schemas[tableName] = list()
            for column in table["columns"]:
                datatype = column["mysql:datatype"]
                nullable = column["nullable"] if "nullable" in column else True
                nullstring = " DEFAULT NULL" if nullable else " NOT NULL"
                schemas[tableName].append(
                    {"name": column["name"],
                     "type": column["mysql:datatype"] + nullstring})
        setattr(namespace, self.dest, schemas)
