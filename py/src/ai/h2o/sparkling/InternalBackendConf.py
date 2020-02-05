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

    def num_h2o_workers(self):
        return self._get_option(self._jconf.numH2OWorkers())

    def drdd_mul_factor(self):
        return self._jconf.drddMulFactor()

    def num_rdd_retries(self):
        return self._jconf.numRddRetries()

    def default_cloud_size(self):
        return self._jconf.defaultCloudSize()

    def subseq_tries(self):
        return self._jconf.subseqTries()

    def node_iced_dir(self):
        return self._get_option(self._jconf.nodeIcedDir())

    #
    # Setters
    #

    def set_num_h2o_workers(self, num_workers):
        self._jconf.setNumH2OWorkers(num_workers)
        return self

    def set_drdd_mul_factor(self, factor):
        self._jconf.setDrddMulFactor(factor)
        return self

    def set_num_rdd_retries(self, retries):
        self._jconf.setNumRddRetries(retries)
        return self

    def set_default_cloud_size(self, size):
        self._jconf.setDefaultCloudSize(size)
        return self

    def set_subseq_tries(self, subseq_tries_num):
        self._jconf.setSubseqTries(subseq_tries_num)
        return self

    def set_node_iced_dir(self, dir):
        self._jconf.setNodeIcedDir(dir)
        return self

