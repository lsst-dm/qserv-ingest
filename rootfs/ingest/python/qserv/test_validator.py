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
Tools used by validation algorithms

@author  Fabrice Jammes, IN2P3
"""

# -------------------------------
#  Imports of standard modules --
# -------------------------------
import sys

# ----------------------------
# Imports for other modules --
# ----------------------------
from . import validator
import os
import pytest

# ---------------------------------
# Local non-exported definitions --
# ---------------------------------

_CWD = os.path.dirname(os.path.abspath(__file__))

def test_dircmp():
    """ Check the function which compare files in two directories
    """
    dir1 = os.path.join(_CWD, "testdata", "dbbench-difffiles")
    dir2 = os.path.join(_CWD, "testdata", "dbbench-expected")
    result = validator._dircmp(dir1, dir2)
    assert (result == False)

    dir1 = os.path.join(_CWD, "testdata", "dbbench-diffdata")
    result = validator._dircmp(dir1, dir2)
    assert (result == False)

    dir1 = os.path.join(_CWD, "testdata", "dbbench-ok")
    result = validator._dircmp(dir1, dir2)
    assert (result == True)
