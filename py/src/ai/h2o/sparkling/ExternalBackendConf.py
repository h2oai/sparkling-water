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
        warnings.warn("Method 'h2o_cluster' is deprecated and will be removed in the next major release. Please use 'h2oCluster'.")
        return self.h2oCluster()

    def h2oCluster(self):
        return self._get_option(self._jconf.h2oCluster())

    def h2o_cluster_host(self):
        warnings.warn("Method 'h2o_cluster_host' is deprecated and will be removed in the next major release. Please use 'h2oClusterHost'.")
        return self.h2oClusterHost()

    def h2oClusterHost(self):
        return self._get_option(self._jconf.h2oClusterHost())

    def h2o_cluster_port(self):
        warnings.warn("Method 'h2o_cluster_port' is deprecated and will be removed in the next major release. Please use 'h2oClusterPort'.")
        return self.h2oClusterPort()

    def h2oClusterPort(self):
        return self._get_option(self._jconf.h2oClusterPort())

    def cluster_size(self):
        warnings.warn("Method 'cluster_size' is deprecated and will be removed in the next major release. Please use 'clusterSize'.")
        return self.clusterSize()

    def clusterSize(self):
        return self._get_option(self._jconf.clusterSize())

    def cluster_start_timeout(self):
        warnings.warn("Method 'cluster_start_timeout' is deprecated and will be removed in the next major release. Please use 'clusterStartTimeout'.")
        return self.clusterStartTimeout()

    def clusterStartTimeout(self):
        return self._jconf.clusterStartTimeout()

    def cluster_config_file(self):
        warnings.warn("Method 'cluster_config_file' is deprecated and will be removed in the next major release. Please use 'clusterInfoFile'.")
        return self.clusterInfoFile()

    def clusterInfoFile(self):
        return self._get_option(self._jconf.clusterInfoFile())

    def mapper_xmx(self):
        warnings.warn("Method 'mapper_xmx' is deprecated and will be removed in the next major release. Please use 'mapperXmx'.")
        return self.mapperXmx()

    def mapperXmx(self):
        return self._jconf.mapperXmx()

    def hdfs_output_dir(self):
        warnings.warn("Method 'hdfs_output_dir' is deprecated and will be removed in the next major release. Please use 'HDFSOutputDir'.")
        return self.HDFSOutputDir()

    def HDFSOutputDir(self):
        return self._get_option(self._jconf.HDFSOutputDir())

    def is_auto_cluster_start_used(self):
        warnings.warn("Method 'is_auto_cluster_start_used' is deprecated and will be removed in the next major release. Please use 'isAutoClusterStartUsed'.")
        return self.isAutoClusterStartUsed()

    def isAutoClusterStartUsed(self):
        return self._jconf.isAutoClusterStartUsed()

    def is_manual_cluster_start_used(self):
        warnings.warn("Method 'is_manual_cluster_start_used' is deprecated and will be removed in the next major release. Please use 'isManualClusterStartUsed'.")
        return self.isManualClusterStartUsed()

    def isManualClusterStartUsed(self):
        return self._jconf.isManualClusterStartUsed()

    def cluster_start_mode(self):
        warnings.warn("Method 'cluster_start_mode' is deprecated and will be removed in the next major release. Please use 'clusterStartMode'.")
        return self.clusterStartMode()

    def clusterStartMode(self):
        return self._jconf.clusterStartMode()

    def h2o_driver_path(self):
        warnings.warn("Method 'h2o_driver_path' is deprecated and will be removed in the next major release. Please use 'h2oDriverPath'.")
        return self.h2oDriverPath()

    def h2oDriverPath(self):
        return self._get_option(self._jconf.h2oDriverPath())

    def yarn_queue(self):
        warnings.warn("Method 'yarn_queue' is deprecated and will be removed in the next major release. Please use 'YARNQueue'.")
        return self.YARNQueue()

    def YARNQueue(self):
        return self._get_option(self._jconf.YARNQueue())

    def is_kill_on_unhealthy_cluster_enabled(self):
        warnings.warn("Method 'is_kill_on_unhealthy_cluster_enabled' is deprecated and will be removed in the next major release. Please use 'isKillOnUnhealthyClusterEnabled'.")
        return self.isKillOnUnhealthyClusterEnabled()

    def isKillOnUnhealthyClusterEnabled(self):
        return self._jconf.isKillOnUnhealthyClusterEnabled()

    def kerberos_principal(self):
        warnings.warn("Method 'kerberos_principal' is deprecated and will be removed in the next major release. Please use 'kerberosPrincipal'.")
        return self.kerberosPrincipal()

    def kerberosPrincipal(self):
        return self._get_option(self._jconf.kerberosPrincipal())

    def kerberos_keytab(self):
        warnings.warn("Method 'kerberos_keytab' is deprecated and will be removed in the next major release. Please use 'kerberosKeytab'.")
        return self.kerberosKeytab()

    def kerberosKeytab(self):
        return self._get_option(self._jconf.kerberosKeytab())

    def run_as_user(self):
        warnings.warn("Method 'run_as_user' is deprecated and will be removed in the next major release. Please use 'runAsUser'.")
        return self.runAsUser()

    def runAsUser(self):
        return self._get_option(self._jconf.runAsUser())

    def externalH2ODriverIf(self):
        return self._get_option(self._jconf.externalH2ODriverIf())

    def externalH2ODriverPort(self):
        return self._get_option(self._jconf.externalH2ODriverPort())

    def externalH2ODriverPortRange(self):
        return self._get_option(self._jconf.externalH2ODriverPortRange())

    def externalExtraMemoryPercent(self):
        return self._jconf.externalExtraMemoryPercent()

    def externalBackendStopTimeout(self):
        return self._jconf.externalBackendStopTimeout()

    def externalHadoopExecutable(self):
        return self._jconf.externalHadoopExecutable()

    def externalExtraJars(self):
        return self._get_option(self._jconf.externalExtraJars())

    def externalCommunicationCompression(self):
        return self._jconf.externalCommunicationCompression()

    #
    # Setters
    #

    def set_h2o_cluster(self, ip, port):
        warnings.warn("Method 'set_h2o_cluster' is deprecated and will be removed in the next major release. Please use 'setH2OCluster'.")
        return self.setH2OCluster(ip, port)

    def setH2OCluster(self, ip, port):
        warnings.warn("The method 'setH2OCluster(ip, port)' also sets backend to external. "
                      "This side effect will be removed in the version in 3.32.")
        self._jconf.setH2OCluster(ip, port)
        return self

    def set_cluster_size(self, cluster_size):
        warnings.warn("Method 'set_cluster_size' is deprecated and will be removed in the next major release. Please use 'setClusterSize'.")
        return self.setClusterSize(cluster_size)

    def setClusterSize(self, clusterSize):
        self._jconf.setClusterSize(clusterSize)
        return self

    def set_cluster_start_timeout(self, timeout):
        warnings.warn("Method 'set_cluster_start_timeout' is deprecated and will be removed in the next major release. Please use 'setClusterStartTimeout'.")
        return self.setClusterStartTimeout(timeout)

    def setClusterStartTimeout(self, clusterStartTimeout):
        self._jconf.setClusterStartTimeout(clusterStartTimeout)
        return self

    def set_cluster_config_file(self, path):
        warnings.warn("Method 'set_cluster_config_file' is deprecated and will be removed in the next major release. Please use 'setClusterConfigFile'.")
        return self.setClusterConfigFile(path)

    def setClusterConfigFile(self, path):
        warnings.warn("The method 'setClusterConfigFile' has been deprecated and will be removed in 3.30."
                      " Use the 'setClusterInfoFile' method instead.")
        self.setClusterInfoFile(path)
        return self

    def setClusterInfoFile(self, path):
        self._jconf.setClusterInfoFile(path)
        return self

    def set_mapper_xmx(self, mem):
        warnings.warn("Method 'set_mapper_xmx' is deprecated and will be removed in the next major release. Please use 'setMapperXmx'.")
        return self.setMapperXmx(mem)

    def setMapperXmx(self, mem):
        self._jconf.setMapperXmx(mem)
        return self

    def set_hdfs_output_dir(self, hdfs_output_dir):
        warnings.warn("Method 'set_hdfs_output_dir' is deprecated and will be removed in the next major release. Please use 'setHDFSOutputDir'.")
        return self.setHDFSOutputDir(hdfs_output_dir)

    def setHDFSOutputDir(self, dir):
        self._jconf.setHDFSOutputDir(dir)
        return self

    def use_auto_cluster_start(self):
        warnings.warn("Method 'use_auto_cluster_start' is deprecated and will be removed in the next major release. Please use 'useAutoClusterStart'.")
        return self.useAutoClusterStart()

    def useAutoClusterStart(self):
        self._jconf.useAutoClusterStart()
        return self

    def use_manual_cluster_start(self):
        warnings.warn("Method 'use_manual_cluster_start' is deprecated and will be removed in the next major release. Please use 'useManualClusterStart'.")
        return self.useManualClusterStart()

    def useManualClusterStart(self):
        self._jconf.useManualClusterStart()
        return self

    def set_h2o_driver_path(self, driver_path):
        warnings.warn("Method 'set_h2o_driver_path' is deprecated and will be removed in the next major release. Please use 'setH2ODriverPath'.")
        return self.setH2ODriverPath(driver_path)

    def setH2ODriverPath(self, path):
        self._jconf.setH2ODriverPath(path)
        return self

    def set_yarn_queue(self, queue_name):
        warnings.warn("Method 'set_yarn_queue' is deprecated and will be removed in the next major release. Please use 'setYARNQueue'.")
        return self.setYARNQueue(queue_name)

    def setYARNQueue(self, queueName):
        self._jconf.setYARNQueue(queueName)
        return self

    def set_kill_on_unhealthy_cluster_enabled(self):
        warnings.warn("Method 'set_kill_on_unhealthy_cluster_enabled' is deprecated and will be removed in the next major release. Please use 'setKillOnUnhealthyClusterEnabled'.")
        return self.setKillOnUnhealthyClusterEnabled()

    def setKillOnUnhealthyClusterEnabled(self):
        self._jconf.setKillOnUnhealthyClusterEnabled()
        return self

    def set_kill_on_unhealthy_cluster_disabled(self):
        warnings.warn("Method 'set_kill_on_unhealthy_cluster_disabled' is deprecated and will be removed in the next major release. Please use 'setKillOnUnhealthyClusterDisabled'.")
        return self.setKillOnUnhealthyClusterDisabled()

    def setKillOnUnhealthyClusterDisabled(self):
        self._jconf.setKillOnUnhealthyClusterDisabled()
        return self

    def set_kerberos_principal(self, principal):
        warnings.warn("Method 'set_kerberos_principal' is deprecated and will be removed in the next major release. Please use 'setKerberosPrincipal'.")
        return self.setKerberosPrincipal(principal)

    def setKerberosPrincipal(self, principal):
        self._jconf.setKerberosPrincipal(principal)
        return self

    def set_kerberos_keytab(self, path):
        warnings.warn("Method 'set_kerberos_keytab' is deprecated and will be removed in the next major release. Please use 'setKerberosKeytab'.")
        return self.setKerberosKeytab(path)

    def setKerberosKeytab(self, path):
        self._jconf.setKerberosKeytab(path)
        return self

    def set_run_as_user(self, user):
        warnings.warn("Method 'set_run_as_user' is deprecated and will be removed in the next major release. Please use 'setRunAsUser'.")
        return self.setRunAsUser(user)

    def setRunAsUser(self, user):
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

    def setExternalBackendStopTimeout(self, timeout):
        self._jconf.setExternalBackendStopTimeout(timeout)
        return self

    def setExternalHadoopExecutable(self, executable):
        self._jconf.setExternalHadoopExecutable(executable)
        return self

    def setExternalExtraJars(self, paths):
        self._jconf.setExternalExtraJars(paths)
        return self

    def setExternalCommunicationCompression(self, compression):
        self._jconf.setExternalCommunicationCompression(compression)
        return self
