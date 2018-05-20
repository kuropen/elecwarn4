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

JQ=$(which jq)
if [ "$JQ" = "" ]; then
  echo "jq is required for testing."
  exit 100
fi

PYTHON=$(which python3 || which python)
TESTFILE=$(mktemp)

# Check the output by the application.
$PYTHON elecwarn.py | $JQ . | tee $TESTFILE

# When Travis CI fetches data of Tohoku area, error occurs.
# Because of this, Tohoku area is excluded from error check for while.
cat $TESTFILE | grep -v tohoku | grep Error > /dev/null

# If there is no error, result code for grep is not 0.
# This should be treated as success.
# If there is error(s), result code for grep is 0.
# This should be treated as fail.
[ $? -ne 0 ]; RESULT_CODE=$?

# Cleaning up.
rm -f $TESTFILE

exit $RESULT_CODE
