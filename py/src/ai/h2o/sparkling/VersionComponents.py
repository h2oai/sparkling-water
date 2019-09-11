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

import re

class VersionComponents(object):

    @staticmethod
    def parseFromSparklingWaterVersion(version):
        match = re.search(r"^((\d+\.\d+)\.(\d+))(-(\d+))?-(\d+\.\d+)$", version)
        result = VersionComponents()
        result.fullVersion = match.group(0)
        result.sparklingVersion = match.group(1)
        result.sparklingMajorVersion = match.group(2)
        result.sparklingMinorVersion = match.group(3)
        result.nightlyVersion = match.group(5)
        result.sparkMajorVersion = match.group(6)
        return result

    @staticmethod
    def parseFromPySparkVersion(version):
        match = re.search(r"^(\d+\.\d+)\.(\d+)(\.([0-9A-Za-z]+))?$", version)
        result = VersionComponents()
        result.fullVersion = match.group(0)
        result.sparkMajorVersion = match.group(1)
        result.sparkPatchVersion = match.group(2)
        result.sparkBuildVersion = match.group(4)
        return result
