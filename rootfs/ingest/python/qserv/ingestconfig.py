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

"""Ingest algorithm configuration management

@author  Fabrice Jammes, IN2P3

"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import argparse
import dataclasses
import logging
import os
import sys
from dataclasses import dataclass, fields
from typing import Any, Optional

import yaml

# ----------------------------
# Imports for other modules --
# ----------------------------
from . import http, version
from .loadbalancerurl import LoadBalancedURL, LoadBalancerAlgorithm

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)
_MIN_SUPPORTED_VERSION = 15

CWD = os.path.dirname(os.path.abspath(__file__))
DATADIR = os.path.join(CWD, "testdata")


class IngestConfig:
    """Configuration parameter for ingest client."""

    def __init__(self, yaml: dict):

        ingest_dict = yaml["ingest"]

        # Added in v15
        self.http_write_timeout = ingest_dict.get("http", {}).get(
            "write_timeout", http.DEFAULT_TIMEOUT_WRITE_SEC
        )
        self.http_read_timeout = ingest_dict.get("http", {}).get(
            "read_timeout", http.DEFAULT_TIMEOUT_READ_SEC
        )

        self.servers = ingest_dict["input"]["servers"]
        self.datapath = ingest_dict["input"]["path"]
        if "metadata" in ingest_dict:
            self.metadata_url = ingest_dict["metadata"]["url"]
        else:
            lbAlgo = LoadBalancerAlgorithm(self.servers)
            self.lb_url = LoadBalancedURL(self.datapath, lbAlgo)
            self.metadata_url = self.lb_url.direct_url

        self._check_version(yaml)
        self.data_url = ingest_dict["qserv"]["queue_url"]
        self.query_url = ingest_dict["qserv"]["query_url"]
        self.queue_url = ingest_dict["qserv"]["queue_url"]
        self.replication_url = ingest_dict["qserv"]["replication_url"]

        # Section name changed in v15 from "ingest" -> "ingestservice"
        ingestcfg = ingest_dict.get("ingestservice")
        if ingestcfg is not None:
            # "auto_build_secondary_index" parameter added in v15
            self.ingestservice = IngestServiceConfig(
                auto_build_secondary_index=ingestcfg.get("auto_build_secondary_index"),
                cainfo=ingestcfg.get("cainfo"),
                ssl_verifypeer=ingestcfg.get("ssl_verifypeer"),
                low_speed_limit=ingestcfg.get("low_speed_limit"),
                low_speed_time=ingestcfg.get("low_speed_time"),
                async_proc_limit=ingestcfg.get("async_proc_limit"),
            )
        else:
            self.ingestservice = IngestServiceConfig()

    def _check_version(self, yaml: dict) -> None:
        """Check ingest file version and exit if its value is not supported

        Parameters
        ----------
        yaml : `dict`
            ingest configuration file content
        """
        fileversion = None
        if "version" in yaml:
            fileversion = yaml["version"]

        if fileversion is None or fileversion < _MIN_SUPPORTED_VERSION:
            _LOG.critical(
                "The ingest configuration file (ingest.yaml) version "
                "is not in the range supported by qserv-ingest "
                "(is %s, expected at least %s)",
                fileversion,
                _MIN_SUPPORTED_VERSION,
            )
            sys.exit(1)
        _LOG.info("Ingest configuration file version: %s", fileversion)


@dataclass
class IngestServiceConfig:
    """Configuration parameters for replication/ingest system
    See https://confluence.lsstcorp.org/display/DM/1.+Setting+configuration+parameters # noqa: W505, E501

    Default value for all parameters are kept, in case `None` value is used in
    constructor

    Parameters
    ----------
    cainfo : `str`
        See above url for documentation
        Default value: "/etc/pki/tls/certs/ca-bundle.crt"
    ssl_verifypeer: `int`
        See above url for documentation
        Default value: 1
    low_speed_limit: `int`
        See above url for documentation
        Default value: 60
    low_speed_time: `int`
        See above url for documentation
        Default value: 120
    async_proc_limit: `int`
        See above url for documentation
        Default value: 0
    """

    # Default valuesingest
    auto_build_secondary_index: Optional[int] = None
    cainfo: str = "/etc/pki/tls/certs/ca-bundle.crt"
    ssl_verifypeer: int = 1
    low_speed_limit: Optional[int] = None
    low_speed_time: Optional[int] = None
    async_proc_limit: Optional[int] = None

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
