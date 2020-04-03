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

import os
import subprocess
from ai.h2o.sparkling.Initializer import Initializer


def testSubmissionSparklingImportBeforeSparkCreated():
    cmd = ["python", "examples/tests/pySparklingImportBeforeSparkSessionCreated.py"]
    returnCode = subprocess.call(cmd)
    assert returnCode == 0, "Process ended in a wrong way. It ended with return code " + str(returnCode)


def testSparkSubmitOptsNoJars():
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.app.name=test"
    jar = Initializer._Initializer__get_sw_jar(None)
    Initializer._Initializer__setUpPySparkSubmitArgs()
    propEdited = os.environ["PYSPARK_SUBMIT_ARGS"]
    assert propEdited == "--jars {} --conf spark.app.name=test".format(jar)


def testSparkSubmitOptsWithJars():
    os.environ["PYSPARK_SUBMIT_ARGS"] = "--conf spark.app.name=test --jars     dummy.jar"
    jar = Initializer._Initializer__get_sw_jar(None)
    Initializer._Initializer__setUpPySparkSubmitArgs()
    propEdited = os.environ["PYSPARK_SUBMIT_ARGS"]
    assert propEdited == "--conf spark.app.name=test --jars     {},dummy.jar".format(jar)
