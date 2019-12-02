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
import pytest


def pytest_addoption(parser):
    parser.addoption("--dist", action="store", default="")
    parser.addoption("--spark_conf", action="store", default="")

@pytest.fixture(scope="session")
def dist(request):
    return request.config.getoption("--dist")

@pytest.fixture(scope="session")
def spark_conf(request):
    conf = request.config.getoption("--spark_conf").split()
    m = {}
    for arg in conf:
        split = arg.split("=", 1)
        m[split[0]] = split[1]
    return m
