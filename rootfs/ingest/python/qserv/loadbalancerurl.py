# This file is part of qserv.
#
# Developed for the LSST Data Management System.
# This product includes software developed by the LSST Project
# (https://www.lsst.org).
# See the COPYRIGHT file at the top-level directory of this distribution
# for details of code ownership.
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
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

"""
Manage metadata related to input data

@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
from __future__ import annotations
from dataclasses import dataclass
import logging
from typing import List
import urllib.parse

# ----------------------------
# Imports for other modules --
# ----------------------------

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------
_LOG = logging.getLogger(__name__)


class LoadBalancerAlgorithm:
    count: int
    loadbalancers: List[str]

    def __init__(self, loadbalancers: List[str]):
        self.count = 0
        self.loadbalancers = loadbalancers

    def get(self) -> str:
        loadbalancers_count = len(self.loadbalancers)
        if loadbalancers_count == 0:
            url = None
        else:
            url = self.loadbalancers[self.count % loadbalancers_count]
            self.count += 1
        return url


class LoadBalancedURL:
    """Manage http(s) load balanced URL
    Also file file:// protocol, and use it as default if no scheme is provided
    """

    def __init__(self, path: str, lbAlgo: LoadBalancerAlgorithm):
        """Manage a load balanced URL

        Args:
            path (str): path of the url
            loadbalancers (List[str], optional): List of http(s) load balancer urls. Defaults to [].

        Raises:
            ValueError: if path uses an unsupported protocol
        """

        if len(lbAlgo.loadbalancers) != 0:
            self.direct_url = urllib.parse.urljoin(lbAlgo.loadbalancers[0], path)
        else:
            self.direct_url = path

        url = urllib.parse.urlsplit(self.direct_url, scheme="file")
        self.counter = lbAlgo
        self.url_path = url.path
        self.loadBalancerAlgorithm = None
        if url.scheme in ["http", "https"]:
            self.loadBalancerAlgorithm = lbAlgo
        elif url.scheme != "file":
            raise ValueError("Unsupported scheme for URL: %s, %s", path, lbAlgo)

    def __repr__(self):
        return f"LoadBalancedURL({self.__dict__})"

    def get(self) -> str:
        lbUrl = self.loadBalancerAlgorithm.get()
        if lbUrl is None:
            url = self.direct_url
        else:
            url = urllib.parse.urljoin(lbUrl, self.url_path)
        return url

    @classmethod
    def new(cls, lb_url: LoadBalancedURL, filepath: str) -> LoadBalancedURL:
        url_path = lb_url.url_path.rstrip("/") + "/" + filepath.strip("/")
        return cls(url_path, lb_url.loadBalancerAlgorithm)
