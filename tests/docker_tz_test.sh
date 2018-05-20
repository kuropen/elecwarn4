#!/usr/bin/env bash

# Test script for Electricity Warning Crawler, Python Edition.
# Copyright (C) 2018 Hirochika Yuda, a.k.a. Kuropen.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

cd $(dirname $0)
cd ..

TESTFILE=$(mktemp)

docker run --rm kuropen/elecwarn-don:testing LANG=C date | tee $TESTFILE
grep JST $TESTFILE > /dev/null

# Fails if JST is not included (timezone is wrong)
exit $?
