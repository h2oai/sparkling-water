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

from ai.h2o.sparkling.VersionComponents import VersionComponents


def testVersionComponentsCanBeParsedFromRegularVersion():
    version = "3.26.2-2.4"
    components = VersionComponents.parseFromSparklingWaterVersion(version)

    assert components.fullVersion == version
    assert components.sparklingVersion == "3.26.2"
    assert components.sparklingMajorVersion == "3.26"
    assert components.sparklingMinorVersion == "2"
    assert components.nightlyVersion is None
    assert components.sparkMajorMinorVersion == "2.4"


def testVersionComponentsCanBeParsedFromNightlyVersion():
    version = "3.28.1-14-2.3"

    components = VersionComponents.parseFromSparklingWaterVersion(version)

    assert components.fullVersion == version
    assert components.sparklingVersion == "3.28.1"
    assert components.sparklingMajorVersion == "3.28"
    assert components.sparklingMinorVersion == "1"
    assert components.nightlyVersion == "14"
    assert components.sparkMajorMinorVersion == "2.3"

def testParseSparkVersion():
    version = "2.4.1"

    components = VersionComponents.parseFromPySparkVersion(version)

    assert components.fullVersion == version
    assert components.sparkMajorMinorVersion == "2.4"
    assert components.sparkMajorVersion == "2"
    assert components.sparkMinorVersion == "4"
    assert components.sparkPatchVersion == "1"
    assert components.sparkBuildVersion is None

def testParseSparkVersionWithBuildNumber():
    version = "2.4.1.dev0"

    components = VersionComponents.parseFromPySparkVersion(version)

    assert components.fullVersion == version
    assert components.sparkMajorMinorVersion == "2.4"
    assert components.sparkMajorVersion == "2"
    assert components.sparkMinorVersion == "4"
    assert components.sparkBuildVersion == "dev0"
