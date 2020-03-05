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

import warnings
from ai.h2o.sparkling.SharedBackendConfUtils import SharedBackendConfUtils
import warnings

class InternalBackendConf(SharedBackendConfUtils):

    #
    # Getters
    #

    def num_h2o_workers(self):
        warnings.warn("Method 'num_h2o_workers' is deprecated and will be removed in the next major release. Please use 'numH2OWorkers'.")
        return self.numH2OWorkers()

    def numH2OWorkers(self):
        return self._get_option(self._jconf.numH2OWorkers())

    def drdd_mul_factor(self):
        warnings.warn("Method 'drdd_mul_factor' is deprecated and will be removed in the next major release. Please use 'drddMulFactor'.")
        return self.drddMulFactor()

    def drddMulFactor(self):
        return self._jconf.drddMulFactor()

    def num_rdd_retries(self):
        warnings.warn("Method 'num_rdd_retries' is deprecated and will be removed in the next major release. Please use 'numRddRetries'.")
        return self.numRddRetries()

    def numRddRetries(self):
        return self._jconf.numRddRetries()

    def default_cloud_size(self):
        warnings.warn("Method 'default_cloud_size' is deprecated and will be removed in the next major release. Please use 'defaultCloudSize'.")
        return self.defaultCloudSize()

    def defaultCloudSize(self):
        return self._jconf.defaultCloudSize()

    def subseq_tries(self):
        warnings.warn("Method 'subseq_tries' is deprecated and will be removed in the next major release. Please use 'subseqTries'.")
        return self.subseqTries()

    def subseqTries(self):
        return self._jconf.subseqTries()

    def h2o_node_web_enabled(self):
        warnings.warn("Method 'h2o_node_web_enabled' is deprecated and will be removed in the next major release. Please use 'h2oNodeWebEnabled'.")
        return self.h2oNodeWebEnabled()

    def h2oNodeWebEnabled(self):
        warnings.warn("Method 'h2oNodeWebEnabled' is deprecated and will be removed in the next major release 3.30'.")
        return self._jconf.h2oNodeWebEnabled()

    def node_iced_dir(self):
        warnings.warn("Method 'node_iced_dir' is deprecated and will be removed in the next major release. Please use 'nodeIcedDir'.")
        return self.nodeIcedDir()

    def nodeIcedDir(self):
        return self._get_option(self._jconf.nodeIcedDir())

    def hdfsConf(self):
        return self._get_option(self._jconf.hdfsConf())

    #
    # Setters
    #

    def set_num_h2o_workers(self, num_workers):
        warnings.warn("Method 'set_num_h2o_workers' is deprecated and will be removed in the next major release. Please use 'setNumH2OWorkers'.")
        return self.setNumH2OWorkers(num_workers)

    def setNumH2OWorkers(self, numWorkers):
        self._jconf.setNumH2OWorkers(numWorkers)
        return self

    def set_drdd_mul_factor(self, factor):
        warnings.warn("Method 'set_drdd_mul_factor' is deprecated and will be removed in the next major release. Please use 'setDrddMulFactor'.")
        return self.setDrddMulFactor(factor)

    def setDrddMulFactor(self, factor):
        self._jconf.setDrddMulFactor(factor)
        return self

    def set_num_rdd_retries(self, retries):
        warnings.warn("Method 'set_num_rdd_retries' is deprecated and will be removed in the next major release. Please use 'setNumRddRetries'.")
        return self.setNumRddRetries(retries)

    def setNumRddRetries(self, retries):
        self._jconf.setNumRddRetries(retries)
        return self

    def set_default_cloud_size(self, size):
        warnings.warn("Method 'set_default_cloud_size' is deprecated and will be removed in the next major release. Please use 'setDefaultCloudSize'.")
        return self.setDefaultCloudSize(size)

    def setDefaultCloudSize(self, defaultClusterSize):
        self._jconf.setDefaultCloudSize(defaultClusterSize)
        return self

    def set_subseq_tries(self, subseq_tries_num):
        warnings.warn("Method 'set_subseq_tries' is deprecated and will be removed in the next major release. Please use 'setSubseqTries'.")
        return self.setSubseqTries(subseq_tries_num)

    def setSubseqTries(self, subseqTriesNum):
        self._jconf.setSubseqTries(subseqTriesNum)
        return self

    def set_h2o_node_web_enabled(self):
        warnings.warn("Method 'set_h2o_node_web_enabled' is deprecated and will be removed in the next major release. Please use 'setH2ONodeWebEnabled'.")
        return self.setH2ONodeWebEnabled()

    def setH2ONodeWebEnabled(self):
        warnings.warn("Method 'setH2ONodeWebEnabled' is deprecated and will be removed in the next major release 3.30'.")
        self._jconf.setH2ONodeWebEnabled()
        return self

    def set_h2o_node_web_disabled(self):
        warnings.warn("Method 'set_h2o_node_web_disabled' is deprecated and will be removed in the next major release. Please use 'setH2ONodeWebDisabled'.")
        return self.setH2ONodeWebDisabled()

    def setH2ONodeWebDisabled(self):
        warnings.warn("Method 'setH2ONodeWebDisabled' is deprecated and will be removed in the next major release 3.30'.")
        self._jconf.setH2ONodeWebDisabled()
        return self

    def set_node_iced_dir(self, dir):
        warnings.warn("Method 'set_node_iced_dir' is deprecated and will be removed in the next major release. Please use 'setNodeIcedDir'.")
        return self.setNodeIcedDir(dir)

    def setNodeIcedDir(self, dir):
        self._jconf.setNodeIcedDir(dir)
        return self

    def setHdfsConf(self, hdfsConf):
        self._jconf.setHdfsConf(hdfsConf)
        return self
