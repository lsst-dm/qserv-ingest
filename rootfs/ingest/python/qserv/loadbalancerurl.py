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


class LoadBalancedURL:
    """Manage http(s) load balanced URL
    Also file file:// protocol, and use it as default if no scheme is provided
    """

    count = 0

    def __init__(self, path: str, loadbalancers: List[str] = []):
        """Manage a load balanced URL

        Args:
            path (str): path of the url
            loadbalancers (List[str], optional): List of http(s) load balancer urls. Defaults to [].

        Raises:
            ValueError: if path uses an unsupported protocol
        """

        if len(loadbalancers) != 0:
            self.direct_url = urllib.parse.urljoin(loadbalancers[0], path)
        else:
            self.direct_url = path

        url = urllib.parse.urlsplit(self.direct_url, scheme="file")
        self.url_path = url.path
        self.loadbalancers = []
        if url.scheme in ["http", "https"]:
            self.loadbalancers = loadbalancers
        elif url.scheme != "file":
            raise ValueError("Unsupported scheme for URL: %s, %s", path, loadbalancers)

    def __repr__(self):
        return f"LoadBalancedURL({self.__dict__})"

    def get(self) -> str:
        loadbalancers_count = len(self.loadbalancers)
        if loadbalancers_count == 0:
            url = self.direct_url
        else:
            url = urllib.parse.urljoin(self.loadbalancers[self.count % loadbalancers_count], self.url_path)
            self.count += 1
        return url

    @classmethod
    def new(cls, lb_url: LoadBalancedURL, filepath: str) -> LoadBalancedURL:
        url_path = lb_url.url_path.rstrip("/") + "/" + filepath.strip("/")
        return cls(url_path, lb_url.loadbalancers)
