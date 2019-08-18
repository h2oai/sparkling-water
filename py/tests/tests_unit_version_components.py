#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys
import os

sys.path.insert(0, sys.argv[1])
os.environ['PYSPARK_PYTHON'] = sys.executable
import unittest

import generic_test_utils
from pysparkling import VersionComponents


class VersionComponentsTestSuite(unittest.TestCase):

    def testVersionComponentsCanBeParsedFromRegularVersion(self):
        version = "3.26.2-2.4"

        components = VersionComponents.parseFromVersion(version)

        assert components.fullVersion == version
        assert components.sparklingVersion == "3.26.2"
        assert components.sparklingMajorVersion == "3.26"
        assert components.sparklingMinorVersion == "2"
        assert components.nightlyVersion is None
        assert components.sparkVersion == "2.4"


    def testVersionComponentsCanBeParsedFromNightlyVersion(self):
        version = "3.28.1-14-2.3"

        components = VersionComponents.parseFromVersion(version)

        assert components.fullVersion == version
        assert components.sparklingVersion == "3.28.1"
        assert components.sparklingMajorVersion == "3.28"
        assert components.sparklingMinorVersion == "1"
        assert components.nightlyVersion == "14"
        assert components.sparkVersion == "2.3"


if __name__ == '__main__':
    generic_test_utils.run_tests([VersionComponentsTestSuite], file_name="py_unit_tests_version_components_report")
