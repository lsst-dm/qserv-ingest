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
import dataclasses
import json
import logging
from dataclasses import dataclass, fields
from typing import Any, Dict, List

import yaml

# ----------------------------
# Imports for other modules --
# ----------------------------


# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)


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


class IngestConfig:
    """Configuration parameter for ingest client."""

    def __init__(self, yaml: dict):

        ingest_dict = yaml["ingest"]

        self.servers = ingest_dict["input"]["servers"]
        self.path = ingest_dict["input"]["path"]
        self.data_url = ingest_dict["qserv"]["queue_url"]
        self.query_url = ingest_dict["qserv"]["query_url"]
        self.queue_url = ingest_dict["qserv"]["queue_url"]
        self.replication_url = ingest_dict["qserv"]["replication_url"]
        replication = ingest_dict.get("replication")
        if replication is not None:
            self.replication_config = ReplicationConfig(
                replication.get("cainfo"),
                replication.get("ssl_verifypeer"),
                replication.get("low_speed_limit"),
                replication.get("low_speed_time"),
            )
        else:
            self.replication_config = ReplicationConfig()


@dataclass
class ReplicationConfig:
    """Configuration parameters for replication/ingest system See https://confl
    uence.lsstcorp.org/display/DM/Ingest%3A+11.1.8.1.+Setting+configuration+par
    ameters.

    Default value for all parameters are kept, in case `None` value is used in
    constructor

    Parameters
    ----------
    cainfo : `str`
        This attribute directly maps to
        https://curl.se/libcurl/c/CURLOPT_PROXY_CAINFO.html.
        Putting the empty string as a value of the parameter will effectively
        turn this option off as if it has never been configured
        for the database.
        Default value: "/etc/pki/tls/certs/ca-bundle.crt"
    ssl_verifypeer: `int`
        This attribute directly maps to
        https://curl.se/libcurl/c/CURLOPT_PROXY_SSL_VERIFYPEER.html.
        Numeric values of the parameter are treated as boolean variables,
        where 0 represents false and any other values represent true.
        Default value: 1
    low_speed_limit: `int`
        This attribute directly maps to
        https://curl.se/libcurl/c/CURLOPT_LOW_SPEED_LIMIT.html
        Putting 0  as a value of the parameter will effectively turn
        this option off as if it has never been configured for the database.
        Default value: 60
    low_speed_time: `int`
        This attribute directly maps to
        https://curl.se/libcurl/c/CURLOPT_LOW_SPEED_TIME.html
        Putting 0  as a value of the parameter will effectively turn this
        option off as if it has never been configured for the database.
        Default value: 120

    """

    cainfo: str = "/etc/pki/tls/certs/ca-bundle.crt"
    ssl_verifypeer: int = 1
    low_speed_limit: int = 60
    low_speed_time: int = 120

    def __post_init__(self) -> None:
        """Set default value for all parameters, in case `None` value is used
        in constructor."""
        for field in fields(self):
            if not isinstance(field.default, dataclasses._MISSING_TYPE) and getattr(self, field.name) is None:
                setattr(self, field.name, field.default)


class IngestConfigAction(argparse.Action):
    """Argparse action to read an ingest client configuration file."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: str = None,
    ) -> None:
        try:
            yaml_data = yaml.safe_load(values)
            config = IngestConfig(yaml_data)
        finally:
            values.close()
        setattr(namespace, self.dest, config)


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


class DictAction(argparse.Action):
    """Argparse action to attempt casting the values to floats and put into a
    dict."""

    def __call__(
        self,
        parser: argparse.ArgumentParser,
        namespace: argparse.Namespace,
        values: Any,
        option_string: str = None,
    ) -> None:
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
