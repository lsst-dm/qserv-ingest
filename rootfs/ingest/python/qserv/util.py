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

"""Tools used by ingest algorithm.

@author  Fabrice Jammes, IN2P3

"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import argparse
import json
import logging
import os
from typing import Any, Dict, List

import yaml

# ----------------------------
# Imports for other modules --
# ----------------------------
from .ingestconfig import IngestConfigAction

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)
CWD = os.path.dirname(os.path.abspath(__file__))
DATADIR = os.path.join(CWD, "testdata")


def add_default_arguments(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--config",
        help="Configuration file for ingest client",
        type=argparse.FileType("r"),
        action=IngestConfigAction,
        metavar="FILE",
        required=True,
    )
    parser.add_argument(
        "-v",
        "--verbose",
        default=0,
        action="count",
        help="More verbose output, can use several times. "
        + "Overridden by QSERV_INGEST_VERBOSE environment variable",
    )


def configure_logger(level: int) -> logging.Logger:
    """Create, configure and returns logger.

    Parameters
    ----------
    level: `int`
        logging level, 0: WARNING, 1: INFO, 2:DEBUG

    Returns
    -------
    logger: `logging.Logger`
        a configured logger which logs on standard output

    """
    levels = {0: logging.WARNING, 1: logging.INFO, 2: logging.DEBUG}
    logger = logging.getLogger()
    loglevel = levels.get(level, logging.DEBUG)
    logger.setLevel(loglevel)
    streamHandler = logging.StreamHandler()
    formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
    streamHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)
    return logger


def trailing_slash(url: str) -> str:
    if not url.endswith("/"):
        url += "/"
    return url


class BaseUrlAction(argparse.Action):
    """Add trailing slash to url."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: str = None,
    ) -> None:
        x = trailing_slash(values)
        setattr(namespace, self.dest, x)


class JsonFileAction(argparse.Action):
    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: str = None,
    ) -> None:
        with open(values, "r") as f:
            x = json.load(f)
        setattr(namespace, self.dest, x)


class FelisAction(argparse.Action):
    """Argparse action to read a felis file into namespace."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: str = None,
    ) -> None:
        with open(values, "r") as f:
            tables = yaml.safe_load(f)["tables"]
        schemas: Dict[str, List] = dict()
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


def increase_wait_time(wait_sec: int) -> int:
    if wait_sec < 10:
        wait_sec *= 2
    return wait_sec


def check_raise(e: Exception, not_raise_msgs: List[str]) -> None:
    """Raise not recognized exceptions, depending on their message.

    Parameters
    ----------
    e : `Exception`
        Exception
    not_raise_msgs : `List[str]`
        List of exception message which prevent raising exception

    Raises
    ------
    Exception
        Raised exception

    """
    raiseExc = True
    for msg in not_raise_msgs:
        if msg in str(e):
            raiseExc = False
            break
    if raiseExc:
        raise e
