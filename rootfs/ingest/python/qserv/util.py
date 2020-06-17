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

# ----------------------------
# Imports for other modules --
# ----------------------------
import requests

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

TMP_DIR = "/tmp"
_LOG = logging.getLogger(__name__)

def download_file(base_url, filename):
    base_url = trailing_slash(base_url)
    file_url = urllib.parse.urljoin(base_url, filename)
    _LOG.debug("Download %s", file_url)
    r = requests.get(file_url)
    abs_filename = os.path.join(TMP_DIR, filename)
    with open(abs_filename, 'wb') as f:
        f.write(r.content)

    if (r.status_code != 200):
        _LOG.fatal("Unable to download file, error %s", r.status_code)
        raise Exception('Unable to download file', file_url, r.status_code)
    return abs_filename


def json_response(base_url, filename):
    """Load json file at a given URL
    """
    file_url = urllib.parse.urljoin(base_url, filename)
    r = requests.get(file_url)
    return r.json()


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
