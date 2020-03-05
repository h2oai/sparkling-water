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

from ai.h2o.sparkling.SharedBackendConfUtils import SharedBackendConfUtils

class InternalBackendConf(SharedBackendConfUtils):

    #
    # Getters
    #

    def numH2OWorkers(self):
        return self._get_option(self._jconf.numH2OWorkers())

    def drddMulFactor(self):
        return self._jconf.drddMulFactor()

    def numRddRetries(self):
        return self._jconf.numRddRetries()

    def defaultCloudSize(self):
        return self._jconf.defaultCloudSize()

    def subseqTries(self):
        return self._jconf.subseqTries()

    def nodeIcedDir(self):
        return self._get_option(self._jconf.nodeIcedDir())

    def hdfsConf(self):
        return self._get_option(self._jconf.hdfsConf())

    #
    # Setters
    #

    def setNumH2OWorkers(self, numWorkers):
        self._jconf.setNumH2OWorkers(numWorkers)
        return self

    def setDrddMulFactor(self, factor):
        self._jconf.setDrddMulFactor(factor)
        return self

    def setNumRddRetries(self, retries):
        self._jconf.setNumRddRetries(retries)
        return self

    def setDefaultCloudSize(self, defaultClusterSize):
        self._jconf.setDefaultCloudSize(defaultClusterSize)
        return self

    def setSubseqTries(self, subseqTriesNum):
        self._jconf.setSubseqTries(subseqTriesNum)
        return self

    def setNodeIcedDir(self, dir):
        self._jconf.setNodeIcedDir(dir)
        return self

    def setHdfsConf(self, hdfsConf):
        self._jconf.setHdfsConf(hdfsConf)
        return self
