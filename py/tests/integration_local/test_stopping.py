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

from tests.integ_test_utils import *
import time

def testStoppingWithoutExplicitStop(integ_spark_conf):
    return_code = launch(integ_spark_conf, "examples/scripts/tests/H2OContextWithoutExplicitStop.py")
    time.sleep(10)
    assert "Total number of applications (application-types: [] and states: [SUBMITTED, ACCEPTED, RUNNING]):0" in listYarnApps()
    assert return_code == 0, "Process ended in a wrong way. It ended with return code " + str(return_code)

def testStoppingWithExplicitStop(integ_spark_conf):
    return_code = launch(integ_spark_conf, "examples/scripts/tests/H2OContextWithExplicitStop.py")
    time.sleep(10)
    assert "Total number of applications (application-types: [] and states: [SUBMITTED, ACCEPTED, RUNNING]):0" in listYarnApps()
    assert return_code == 0, "Process ended in a wrong way. It ended with return code " + str(return_code)
