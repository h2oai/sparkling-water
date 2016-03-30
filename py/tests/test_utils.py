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

import unittest
import os


# Runs python tests and by default reports to console.
# If filename is specified it additionally reports output to file with that name into py/build directory
def run_tests(suite, file_name=None):
    if file_name is not None:
        reports_file = 'build'+os.sep+file_name+".txt"
        f = open(reports_file, "w")
        testsuite = unittest.TestLoader().loadTestsFromTestCase(suite)
        unittest.TextTestRunner(f, verbosity=2).run(testsuite)
        f.close()

        # Print output to console without running the tests again
        with open(reports_file, 'r') as fin:
            print fin.read()
    else:
        # Run tests and print to console
        testsuite = unittest.TestLoader().loadTestsFromTestCase(suite)
        unittest.TextTestRunner(verbosity=2).run(testsuite)