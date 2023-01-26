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

"""Tools used by validation algorithms.

@author  Fabrice Jammes, IN2P3

"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import os

# ----------------------------
# Imports for other modules --
# ----------------------------
from . import util, validator

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------


def test_dircmp() -> None:
    """Check the function which compare files in two directories."""
    dir1 = os.path.join(util.DATADIR, "dbbench-difffiles")
    dir2 = os.path.join(util.DATADIR, "dbbench-expected")
    result = validator._dircmp(dir1, dir2)
    assert not result

    dir1 = os.path.join(util.DATADIR, "dbbench-diffdata")
    result = validator._dircmp(dir1, dir2)
    assert not result

    dir1 = os.path.join(util.DATADIR, "dbbench-ok")
    result = validator._dircmp(dir1, dir2)
    assert result
