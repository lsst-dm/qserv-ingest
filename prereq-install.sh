#!/bin/bash

# LSST Data Management System
# Copyright 2014 LSST Corporation.
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

# Install argo client 

# @author Fabrice Jammes SLAC/IN2P3

set -euxo pipefail

ARGOPROJ_SRC=$(mktemp -d /tmp/argoproj-helper.XXXXXX)
curl -sL https://raw.githubusercontent.com/helm/helm/master/scripts/get-helm-3 | bash -s -- --version v3.5.4

ARGOPROJ_HELPER_VERSION="v1.0.1"
git clone --depth 1 -b "$ARGOPROJ_HELPER_VERSION" --single-branch https://github.com/k8s-school/argoproj-helper.git "$ARGOPROJ_SRC"

"$ARGOPROJ_SRC"/workflow/install.sh -j
"$ARGOPROJ_SRC"/argo-client-install.sh
