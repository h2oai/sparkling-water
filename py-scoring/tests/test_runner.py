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
import sys
import pytest

dist = sys.argv[2]
path = os.getenv("PYTHONPATH")
if path is None:
    path = dist
else:
    path = "{}:{}".format(dist, path)

os.putenv("PYTHONPATH", path)
os.putenv('PYSPARK_DRIVER_PYTHON', sys.executable)
os.putenv('PYSPARK_PYTHON', sys.executable)

os.environ["PYTHONPATH"] = path
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
os.environ['PYSPARK_PYTHON'] = sys.executable

sys.path.insert(0, dist)
pytestConfigArgs = sys.argv[1].replace("'", "").split(" ")
args = pytestConfigArgs + ["-s", "--dist", dist, "--spark_conf", ' '.join(sys.argv[3:])]
code = pytest.main(args)
sys.exit(code)
