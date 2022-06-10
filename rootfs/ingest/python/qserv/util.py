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
import io
import json
import logging
import yaml
import time

# ----------------------------
# Imports for other modules --
# ----------------------------
from sqlalchemy import event
from sqlalchemy.engine import Engine

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)

@event.listens_for(Engine, "before_cursor_execute")
def before_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    conn.info.setdefault("query_start_time", []).append(time.time())
    _LOG.debug("Query: %s", statement)


@event.listens_for(Engine, "after_cursor_execute")
def after_cursor_execute(conn, cursor, statement, parameters, context, executemany):
    total = time.time() - conn.info["query_start_time"].pop(-1)
    _LOG.debug("Query total time: %f", total)


def add_default_arguments(parser: argparse.ArgumentParser):
    parser.add_argument(
        "--config",
        help="Configuration file for ingest client",
        type=argparse.FileType("r"),
        action=IngestConfigAction,
        metavar="FILE",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Use debug logging"
    )


def get_default_logger(verbose):
    """
    Create and returns default logger
    """
    logger = logging.getLogger()
    if verbose:
        logger.setLevel(logging.DEBUG)
    else:
        logger.setLevel(logging.INFO)
    streamHandler = logging.StreamHandler()
    formatter = logging.Formatter(
        "%(asctime)s - %(name)s - %(levelname)s - %(message)s"
    )
    streamHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)
    return logger


def trailing_slash(url):
    if not url.endswith("/"):
        url += "/"
    return url


class IngestConfig:
    """
    Configuration parameter for ingest client
    """

    def __init__(self, yaml: dict):
        self.servers = yaml["ingest"]["input"]["servers"]
        self.path = yaml["ingest"]["input"]["path"]
        self.data_url = yaml["ingest"]["qserv"]["queue_url"]
        self.query_url = yaml["ingest"]["qserv"]["query_url"]
        self.queue_url = yaml["ingest"]["qserv"]["queue_url"]
        self.replication_url = yaml["ingest"]["qserv"]["replication_url"]


class IngestConfigAction(argparse.Action):
    """
    Argparse action to read an ingest client configuration file
    """

    def __call__(self, parser, namespace, values, option_string=None):
        try:
            yaml_data = yaml.safe_load(values)
            config = IngestConfig(yaml_data)
        finally:
            values.close()
        setattr(namespace, self.dest, config)


class BaseUrlAction(argparse.Action):
    """
    Add trailing slash to url
    """

    def __call__(self, parser, namespace, values, option_string):
        x = trailing_slash(values)
        setattr(namespace, self.dest, x)


class DictAction(argparse.Action):
    """
    Argparse action to attempt casting the values to floats and put into a dict
    """

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
        with open(values, "r") as f:
            x = json.load(f)
        setattr(namespace, self.dest, x)


class FelisAction(argparse.Action):
    """
    Argparse action to read a felis file into namespace
    """

    def __call__(self, parser, namespace, values, option_string):
        with open(values, "r") as f:
            tables = yaml.safe_load(f)["tables"]
        schemas = dict()
        for table in tables:
            tableName = table["name"]
            schemas[tableName] = list()
            for column in table["columns"]:
                nullable = column["nullable"] if "nullable" in column else True
                nullstring = " DEFAULT NULL" if nullable else " NOT NULL"
                schemas[tableName].append(
                    {
                        "name": column["name"],
                        "type": column["mysql:datatype"] + nullstring,
                    }
                )
        setattr(namespace, self.dest, schemas)


def increase_wait_time(wait_sec):
    if wait_sec < 10:
        wait_sec *= 2
    return wait_sec
