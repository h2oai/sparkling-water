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

class SharedBackendConf(SharedBackendConfUtils):

    #
    # Getters
    #

    def backendClusterMode(self):
        return self._jconf.backendClusterMode()

    def cloudName(self):
        return self._get_option(self._jconf.cloudName())

    def nthreads(self):
        return self._jconf.nthreads()

    def isH2OReplEnabled(self):
        return self._jconf.isH2OReplEnabled()

    def scalaIntDefaultNum(self):
        return self._jconf.scalaIntDefaultNum()

    def isClusterTopologyListenerEnabled(self):
        return self._jconf.isClusterTopologyListenerEnabled()

    def isSparkVersionCheckEnabled(self):
        return self._jconf.isSparkVersionCheckEnabled()

    def isFailOnUnsupportedSparkParamEnabled(self):
        return self._jconf.isFailOnUnsupportedSparkParamEnabled()

    def jks(self):
        return self._get_option(self._jconf.jks())

    def jksPass(self):
        return self._get_option(self._jconf.jksPass())

    def jksAlias(self):
        return self._get_option(self._jconf.jksAlias())

    def hashLogin(self):
        return self._jconf.hashLogin()

    def ldapLogin(self):
        return self._jconf.ldapLogin()

    def kerberosLogin(self):
        return self._jconf.kerberosLogin()

    def loginConf(self):
        return self._get_option(self._jconf.loginConf())

    def userName(self):
        return self._get_option(self._jconf.userName())

    def password(self):
        return self._get_option(self._jconf.password())

    def sslConf(self):
        return self._get_option(self._jconf.sslConf())

    def autoFlowSsl(self):
        return self._jconf.autoFlowSsl()

    def h2oNodeLogLevel(self):
        return self._jconf.h2oNodeLogLevel()

    def h2oNodeLogDir(self):
        return self._jconf.h2oNodeLogDir()

    def backendHeartbeatInterval(self):
        return self._jconf.backendHeartbeatInterval()

    def cloudTimeout(self):
        return self._jconf.cloudTimeout()

    def nodeNetworkMask(self):
        return self._get_option(self._jconf.nodeNetworkMask())

    def stacktraceCollectorInterval(self):
        return self._jconf.stacktraceCollectorInterval()

    def contextPath(self):
        return self._get_option(self._jconf.contextPath())

    def flowScalaCellAsync(self):
        return self._jconf.flowScalaCellAsync()

    def maxParallelScalaCellJobs(self):
        return self._jconf.maxParallelScalaCellJobs()

    def internalPortOffset(self):
        return self._jconf.internalPortOffset()

    def mojoDestroyTimeout(self):
        return self._jconf.mojoDestroyTimeout()

    def nodeBasePort(self):
        return self._jconf.nodeBasePort()

    def nodeExtraProperties(self):
        return self._get_option(self._jconf.nodeExtraProperties())

    def flowExtraHttpHeaders(self):
        return self._get_option(self._jconf.flowExtraHttpHeaders())

    def isInternalSecureConnectionsEnabled(self):
        return self._jconf.isInternalSecureConnectionsEnabled()

    def flowDir(self):
        return self._get_option(self._jconf.flowDir())

    def clientIp(self):
        return self._get_option(self._jconf.clientIp())

    def clientIcedDir(self):
        return self._get_option(self._jconf.clientIcedDir())

    def h2oClientLogLevel(self):
        return self._jconf.h2oClientLogLevel()

    def h2oClientLogDir(self):
        return self._get_option(self._jconf.h2oClientLogDir())

    def clientBasePort(self):
        return self._jconf.clientBasePort()

    def clientWebPort(self):
        return self._jconf.clientWebPort()

    def clientVerboseOutput(self):
        return self._jconf.clientVerboseOutput()

    def clientNetworkMask(self):
        return self._get_option(self._jconf.clientNetworkMask())

    def clientFlowBaseurlOverride(self):
        return self._get_option(self._jconf.clientFlowBaseurlOverride())

    def clientExtraProperties(self):
        return self._get_option(self._jconf.clientExtraProperties())

    def runsInExternalClusterMode(self):
        return self._jconf.runsInExternalClusterMode()

    def runsInInternalClusterMode(self):
        return self._jconf.runsInInternalClusterMode()

    def clientCheckRetryTimeout(self):
        return self._jconf.clientCheckRetryTimeout()

    def verifySslCertificates(self):
        return self._jconf.verifySslCertificates()

    def isHiveSupportEnabled(self):
        return self._jconf.isHiveSupportEnabled()

    def hiveHost(self):
        return self._get_option(self._jconf.hiveHost())

    def hivePrincipal(self):
        return self._get_option(self._jconf.hivePrincipal())

    def hiveJdbcUrlPattern(self):
        return self._get_option(self._jconf.hiveJdbcUrlPattern())

    def hiveToken(self):
        return self._get_option(self._jconf.hiveToken())

    #
    # Setters
    #

    def setInternalClusterMode(self):
        self._jconf.setInternalClusterMode()
        return self

    def setExternalClusterMode(self):
        self._jconf.setExternalClusterMode()
        return self

    def setCloudName(self, cloudName):
        self._jconf.setCloudName(cloudName)
        return self

    def setNthreads(self, nthreads):
        self._jconf.setNthreads(nthreads)
        return self

    def setReplEnabled(self):
        self._jconf.setReplEnabled()
        return self

    def setReplDisabled(self):
        self._jconf.setReplDisabled()
        return self

    def setDefaultNumReplSessions(self, numSessions):
        self._jconf.setDefaultNumReplSessions(numSessions)
        return self

    def setClusterTopologyListenerEnabled(self):
        self._jconf.setClusterTopologyListenerEnabled()
        return self

    def setClusterTopologyListenerDisabled(self):
        self._jconf.setClusterTopologyListenerDisabled()
        return self

    def setSparkVersionCheckEnabled(self):
        self._jconf.setSparkVersionCheckEnabled()
        return self

    def setSparkVersionCheckDisabled(self):
        self._jconf.setSparkVersionCheckDisabled()
        return self

    def setFailOnUnsupportedSparkParamEnabled(self):
        self._jconf.setFailOnUnsupportedSparkParamEnabled()
        return self

    def setFailOnUnsupportedSparkParamDisabled(self):
        self._jconf.setFailOnUnsupportedSparkParamDisabled()
        return self

    def setJks(self, path):
        self._jconf.setJks(path)
        return self

    def setJksPass(self, password):
        self._jconf.setJksPass(password)
        return self

    def setJksAlias(self, alias):
        self._jconf.setJksAlias(alias)
        return self

    def setHashLoginEnabled(self):
        self._jconf.setHashLoginEnabled()
        return self

    def setHashLoginDisabled(self):
        self._jconf.setHashLoginDisabled()
        return self

    def setLdapLoginEnabled(self):
        self._jconf.setLdapLoginEnabled()
        return self

    def setLdapLoginDisabled(self):
        self._jconf.setLdapLoginDisabled()
        return self

    def setKerberosLoginEnabled(self):
        self._jconf.setKerberosLoginEnabled()
        return self

    def setKerberosLoginDisabled(self):
        self._jconf.setKerberosLoginDisabled()
        return self

    def setLoginConf(self, filePath):
        self._jconf.setLoginConf(filePath)
        return self

    def setUserName(self, username):
        self._jconf.setUserName(username)
        return self

    def setPassword(self, password):
        self._jconf.setPassword(password)
        return self

    def setSslConf(self, path):
        self._jconf.setSslConf(path)
        return self

    def setAutoFlowSslEnabled(self):
        self._jconf.setAutoFlowSslEnabled()
        return self

    def setAutoFlowSslDisabled(self):
        self._jconf.setAutoFlowSslDisabled()
        return self

    def setH2ONodeLogLevel(self, level):
        self._jconf.setH2ONodeLogLevel(level)
        return self

    def setH2ONodeLogDir(self, dir):
        self._jconf.setH2ONodeLogDir(dir)
        return self

    def setBackendHeartbeatInterval(self, interval):
        self._jconf.setBackendHeartbeatInterval(interval)
        return self

    def setCloudTimeout(self, timeout):
        self._jconf.setCloudTimeout(timeout)
        return self

    def setNodeNetworkMask(self, mask):
        self._jconf.setNodeNetworkMask(mask)
        return self

    def setStacktraceCollectorInterval(self, interval):
        self._jconf.setStacktraceCollectorInterval(interval)
        return self

    def setContextPath(self, contextPath):
        self._jconf.setContextPath(contextPath)
        return self

    def setFlowScalaCellAsyncEnabled(self):
        self._jconf.setFlowScalaCellAsyncEnabled()
        return self

    def setFlowScalaCellAsyncDisabled(self):
        self._jconf.setFlowScalaCellAsyncDisabled()
        return self

    def setMaxParallelScalaCellJobs(self, limit):
        self._jconf.setMaxParallelScalaCellJobs(limit)
        return self

    def setInternalPortOffset(self, offset):
        self._jconf.setInternalPortOffset(offset)
        return self

    def setNodeBasePort(self, port):
        self._jconf.setNodeBasePort(port)
        return self

    def setMojoDestroyTimeout(self, timeoutInMilliseconds):
        self._jconf.setMojoDestroyTimeout(timeoutInMilliseconds)
        return self

    def setNodeExtraProperties(self, extraProperties):
        self._jconf.setNodeExtraProperties(extraProperties)
        return self

    def setFlowExtraHttpHeaders(self, headers):
        self._jconf.setFlowExtraHttpHeaders(headers)
        return self

    def setInternalSecureConnectionsEnabled(self):
        self._jconf.setInternalSecureConnectionsEnabled()
        return self

    def setInternalSecureConnectionsDisabled(self):
        self._jconf.setInternalSecureConnectionsDisabled()
        return self

    def setFlowDir(self, dir):
        self._jconf.setFlowDir(dir)
        return self

    def setClientIp(self, ip):
        self._jconf.setClientIp(ip)
        return self

    def setClientIcedDir(self, icedDir):
        self._jconf.setClientIcedDir(icedDir)
        return self

    def setH2OClientLogLevel(self, level):
        self._jconf.setH2OClientLogLevel(level)
        return self

    def setH2OClientLogDir(self, dir):
        self._jconf.setH2OClientLogDir(dir)
        return self

    def setClientBasePort(self, basePort):
        self._jconf.setClientBasePort(basePort)
        return self

    def setClientWebPort(self, port):
        self._jconf.setClientWebPort(port)
        return self

    def setClientVerboseEnabled(self):
        self._jconf.setClientVerboseEnabled()
        return self

    def setClientVerboseDisabled(self):
        self._jconf.setClientVerboseDisabled()
        return self

    def setClientNetworkMask(self, mask):
        self._jconf.setClientNetworkMask(mask)
        return self

    def setClientFlowBaseurlOverride(self, baseUrl):
        self._jconf.setClientFlowBaseurlOverride(baseUrl)
        return self

    def setClientCheckRetryTimeout(self, timeout):
        self._jconf.setClientCheckRetryTimeout(timeout)
        return self

    def setClientExtraProperties(self, extraProperties):
        self._jconf.setClientExtraProperties(extraProperties)
        return self

    def setVerifySslCertificates(self, verify):
        self._jconf.setVerifySslCertificates(verify)
        return self

    def setHiveSupportEnabled(self):
        self._jconf.setHiveSupportEnabled()
        return self

    def setHiveSupportDisabled(self):
        self._jconf.setHiveSupportDisabled()
        return self

    def setHiveHost(self, host):
        self._jconf.setHiveHost(host)
        return self

    def setHivePrincipal(self, principal):
        self._jconf.setHivePrincipal(principal)
        return self

    def setHiveJdbcUrlPattern(self, pattern):
        self._jconf.setHiveJdbcUrlPattern(pattern)
        return self

    def setHiveToken(self, token):
        self._jconf.setHiveToken(token)
        return self
