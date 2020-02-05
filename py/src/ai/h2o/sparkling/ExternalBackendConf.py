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


class ExternalBackendConf(SharedBackendConfUtils):

    #
    # Getters
    #

    def h2o_cluster(self):
        return self._get_option(self._jconf.h2oCluster())

    def h2o_cluster_host(self):
        return self._get_option(self._jconf.h2oClusterHost())

    def h2o_cluster_port(self):
        return self._get_option(self._jconf.h2oClusterPort())

    def cluster_size(self):
        return self._get_option(self._jconf.clusterSize())

    def client_check_retry_timeout(self):
        return self._jconf.clientCheckRetryTimeout()

    def external_write_confirmation_timeout(self):
        warnings.warn(
            "Method 'external_write_confirmation_timeout' is deprecated without replacement and will be removed"
            " in the next major release")
        return self._jconf.externalWriteConfirmationTimeout()

    def cluster_start_timeout(self):
        return self._jconf.clusterStartTimeout()

    def cluster_config_file(self):
        return self._get_option(self._jconf.clusterInfoFile())

    def mapper_xmx(self):
        return self._jconf.mapperXmx()

    def hdfs_output_dir(self):
        return self._get_option(self._jconf.HDFSOutputDir())

    def cluster_start_mode(self):
        return self._jconf.clusterStartMode()

    def is_auto_cluster_start_used(self):
        return self._jconf.isAutoClusterStartUsed()

    def is_manual_cluster_start_used(self):
        return self._jconf.isManualClusterStartUsed()

    def h2o_driver_path(self):
        return self._get_option(self._jconf.h2oDriverPath())

    def yarn_queue(self):
        return self._get_option(self._jconf.YARNQueue())

    def is_kill_on_unhealthy_cluster_enabled(self):
        return self._jconf.isKillOnUnhealthyClusterEnabled()

    def kerberos_principal(self):
        return self._get_option(self._jconf.kerberosPrincipal())

    def kerberos_keytab(self):
        return self._get_option(self._jconf.kerberosKeytab())

    def run_as_user(self):
        return self._get_option(self._jconf.runAsUser())

    def externalH2ODriverIf(self):
        return self._get_option(self._jconf.externalH2ODriverIf())

    def externalH2ODriverPort(self):
        return self._get_option(self._jconf.externalH2ODriverPort())

    def externalH2ODriverPortRange(self):
        return self._get_option(self._jconf.externalH2ODriverPortRange())

    def externalExtraMemoryPercent(self):
        return self._jconf.externalExtraMemoryPercent()

    def externalCommunicationBlockSizeAsBytes(self):
        return self._jconf.externalCommunicationBlockSizeAsBytes()

    def externalBackendStopTimeout(self):
        return self._jconf.externalBackendStopTimeout()

    def externalHadoopExecutable(self):
        return self._jconf.externalHadoopExecutable()

    #
    # Setters
    #

    def set_h2o_cluster(self, ip, port):
        self._jconf.setH2OCluster(ip, port)
        return self

    def set_cluster_size(self, cluster_size):
        self._jconf.setClusterSize(cluster_size)
        return self

    def set_client_check_retry_timeout(self, timeout):
        """Set retry interval how often nodes in the external cluster mode check for the presence of the h2o client.

        Arguments:
        timeout -- timeout in milliseconds

        """
        self._jconf.setClientCheckRetryTimeout(timeout)
        return self

    def set_external_write_confirmation_timeout(self, timeout):
        warnings.warn(
            "Method 'set_external_write_confirmation_timeout' is deprecated without replacement and will be removed"
            " in the next major release")
        self._jconf.setExternalWriteConfirmationTimeout(timeout)
        return self

    def set_cluster_start_timeout(self, timeout):
        """Set timeout for start of external cluster. If the cluster is not able to cloud within the timeout the
        the exception is thrown.

        Arguments:
        timeout -- timeout in seconds

        """
        self._jconf.setClusterStartTimeout(timeout)
        return self

    def set_cluster_config_file(self, path):
        self._jconf.setClusterConfigFile(path)
        return self

    def set_mapper_xmx(self, mem):
        self._jconf.setMapperXmx(mem)
        return self

    def set_hdfs_output_dir(self, hdfs_output_dir):
        self._jconf.setHDFSOutputDir(hdfs_output_dir)
        return self

    def use_auto_cluster_start(self):
        self._jconf.useAutoClusterStart()
        return self

    def use_manual_cluster_start(self):
        self._jconf.useManualClusterStart()
        return self

    def set_h2o_driver_path(self, driver_path):
        self._jconf.setH2ODriverPath(driver_path)
        return self

    def set_yarn_queue(self, queue_name):
        self._jconf.setYARNQueue(queue_name)
        return self

    def set_kill_on_unhealthy_cluster_enabled(self):
        self._jconf.setKillOnUnhealthyClusterEnabled()
        return self

    def set_kill_on_unhealthy_cluster_disabled(self):
        self._jconf.setKillOnUnhealthyClusterDisabled()
        return self

    def set_kerberos_principal(self, principal):
        self._jconf.setKerberosPrincipal(principal)
        return self

    def set_kerberos_keytab(self, path):
        self._jconf.setKerberosKeytab(path)
        return self

    def set_run_as_user(self, user):
        self._jconf.setRunAsUser(user)
        return self

    def setExternalH2ODriverIf(self, iface):
        self._jconf.setExternalH2ODriverIf(iface)
        return self

    def setExternalH2ODriverPort(self, port):
        self._jconf.setExternalH2ODriverPort(port)
        return self

    def setExternalH2ODriverPortRange(self, portrange):
        self._jconf.setExternalH2ODriverPortRange(portrange)
        return self

    def setExternalExtraMemoryPercent(self, memoryPercent):
        self._jconf.setExternalExtraMemoryPercent(memoryPercent)
        return self

    def setExternalCommunicationBlockSize(self, blockSize):
        self._jconf.setExternalCommunicationBlockSize(blockSize)
        return self

    def setExternalBackendStopTimeout(self, timeout):
        self._jconf.setExternalBackendStopTimeout(timeout)
        return self

    def setExternalHadoopExecutable(self, executable):
        self._jconf.setExternalHadoopExecutable(executable)
        return self
