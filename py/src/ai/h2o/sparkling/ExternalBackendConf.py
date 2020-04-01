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

    def h2oCluster(self):
        return self._get_option(self._jconf.h2oCluster())

    def h2oClusterHost(self):
        return self._get_option(self._jconf.h2oClusterHost())

    def h2oClusterPort(self):
        return self._get_option(self._jconf.h2oClusterPort())

    def clusterSize(self):
        return self._get_option(self._jconf.clusterSize())

    def clusterStartTimeout(self):
        return self._jconf.clusterStartTimeout()

    def clusterInfoFile(self):
        return self._get_option(self._jconf.clusterInfoFile())

    def mapperXmx(self):
        return self._jconf.mapperXmx()

    def HDFSOutputDir(self):
        return self._get_option(self._jconf.HDFSOutputDir())

    def isAutoClusterStartUsed(self):
        return self._jconf.isAutoClusterStartUsed()

    def isManualClusterStartUsed(self):
        return self._jconf.isManualClusterStartUsed()

    def clusterStartMode(self):
        return self._jconf.clusterStartMode()

    def h2oDriverPath(self):
        return self._get_option(self._jconf.h2oDriverPath())

    def YARNQueue(self):
        return self._get_option(self._jconf.YARNQueue())

    def isKillOnUnhealthyClusterEnabled(self):
        return self._jconf.isKillOnUnhealthyClusterEnabled()

    def kerberosPrincipal(self):
        return self._get_option(self._jconf.kerberosPrincipal())

    def kerberosKeytab(self):
        return self._get_option(self._jconf.kerberosKeytab())

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

    def setH2OCluster(self, ip, port):
        warnings.warn("The method 'setH2OCluster(ip, port)' also sets backend to external. "
                      "This side effect will be removed in the version in 3.32.")
        self._jconf.setH2OCluster(ip, port)
        return self

    def setClusterSize(self, clusterSize):
        self._jconf.setClusterSize(clusterSize)
        return self

    def setClusterStartTimeout(self, clusterStartTimeout):
        self._jconf.setClusterStartTimeout(clusterStartTimeout)
        return self

    def setClusterInfoFile(self, path):
        self._jconf.setClusterInfoFile(path)
        return self

    def setMapperXmx(self, mem):
        self._jconf.setMapperXmx(mem)
        return self

    def setHDFSOutputDir(self, dir):
        self._jconf.setHDFSOutputDir(dir)
        return self

    def useAutoClusterStart(self):
        self._jconf.useAutoClusterStart()
        return self

    def useManualClusterStart(self):
        self._jconf.useManualClusterStart()
        return self

    def setH2ODriverPath(self, path):
        self._jconf.setH2ODriverPath(path)
        return self

    def setYARNQueue(self, queueName):
        self._jconf.setYARNQueue(queueName)
        return self

    def setKillOnUnhealthyClusterEnabled(self):
        self._jconf.setKillOnUnhealthyClusterEnabled()
        return self

    def setKillOnUnhealthyClusterDisabled(self):
        self._jconf.setKillOnUnhealthyClusterDisabled()
        return self

    def setKerberosPrincipal(self, principal):
        self._jconf.setKerberosPrincipal(principal)
        return self

    def setKerberosKeytab(self, path):
        self._jconf.setKerberosKeytab(path)
        return self

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
