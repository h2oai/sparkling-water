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

from pysparkling.conf import H2OConf


def test_non_overloaded_setter_without_arguments():
    conf = H2OConf().useManualClusterStart()
    assert (conf.isManualClusterStartUsed() is True)


def test_non_overloaded_setter_with_argument():
    conf = H2OConf().setExternalMemory("24G")
    assert (conf.externalMemory() == "24G")


def test_overloaded_setter_with_two_arguments():
    conf = H2OConf().setH2OCluster("my_host", 8765)
    assert (conf.h2oClusterHost() == "my_host")
    assert (conf.h2oClusterPort() == 8765)


def test_overloaded_setter_with_one_argument():
    conf = H2OConf().setH2OCluster("my_host:6543")
    assert (conf.h2oClusterHost() == "my_host")
    assert (conf.h2oClusterPort() == 6543)


def test_overloaded_setter_with_string_argument():
    conf = H2OConf().setExternalExtraJars("path1,path2,path3")
    assert (conf.externalExtraJars() == "path1,path2,path3")


def test_overloaded_setter_with_list_argument():
    conf = H2OConf().setExternalExtraJars(["path1", "path2", "path3"])
    assert (conf.externalExtraJars() == "path1,path2,path3")

