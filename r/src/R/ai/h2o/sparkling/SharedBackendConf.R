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

#' @export SharedBackendConf
SharedBackendConf <- setRefClass("SharedBackendConf", contains = ("ConfBase"), methods = list(

#
# Getters
#
    backendClusterMode = function() { invoke(jconf, "backendClusterMode") },

    cloudName = function() { ConfBase.getOption(invoke(jconf, "cloudName")) },

    nthreads = function() { invoke(jconf, "nthreads") },

    isH2OReplEnabled = function() { invoke(jconf, "isH2OReplEnabled") },

    scalaIntDefaultNum = function() { invoke(jconf, "scalaIntDefaultNum") },

    isClusterTopologyListenerEnabled = function() { invoke(jconf, "isClusterTopologyListenerEnabled") },

    isSparkVersionCheckEnabled = function() { invoke(jconf, "isSparkVersionCheckEnabled") },

    isFailOnUnsupportedSparkParamEnabled = function() { invoke(jconf, "isFailOnUnsupportedSparkParamEnabled") },

    jks = function() { ConfBase.getOption(invoke(jconf, "jks")) },

    jksPass = function() { ConfBase.getOption(invoke(jconf, "jksPass")) },

    jksAlias = function() { ConfBase.getOption(invoke(jconf, "jksAlias")) },

    hashLogin = function() { invoke(jconf, "hashLogin") },

    ldapLogin = function() { invoke(jconf, "ldapLogin") },

    kerberosLogin = function() { invoke(jconf, "kerberosLogin") },

    loginConf = function() { ConfBase.getOption(invoke(jconf, "loginConf")) },

    userName = function() { ConfBase.getOption(invoke(jconf, "userName")) },

    password = function() { ConfBase.getOption(invoke(jconf, "password")) },

    sslConf = function() { ConfBase.getOption(invoke(jconf, "sslConf")) },

    autoFlowSsl = function() { invoke(jconf, "autoFlowSsl") },

    h2oNodeLogLevel = function() { invoke(jconf, "h2oNodeLogLevel") },

    h2oNodeLogDir = function() { invoke(jconf, "h2oNodeLogDir") },

    backendHeartbeatInterval = function() { invoke(jconf, "backendHeartbeatInterval") },

    cloudTimeout = function() { invoke(jconf, "cloudTimeout") },

    nodeNetworkMask = function() { ConfBase.getOption(invoke(jconf, "nodeNetworkMask")) },

    stacktraceCollectorInterval = function() { invoke(jconf, "stacktraceCollectorInterval") },

    contextPath = function() { ConfBase.getOption(invoke(jconf, "contextPath")) },

    flowScalaCellAsync = function() { invoke(jconf, "flowScalaCellAsync") },

    maxParallelScalaCellJobs = function() { invoke(jconf, "maxParallelScalaCellJobs") },

    internalPortOffset = function() { invoke(jconf, "internalPortOffset") },

    mojoDestroyTimeout = function() { invoke(jconf, "mojoDestroyTimeout") },

    nodeBasePort = function() { invoke(jconf, "nodeBasePort") },

    nodeExtraProperties = function() { ConfBase.getOption(invoke(jconf, "nodeExtraProperties")) },

    flowExtraHttpHeaders = function() { ConfBase.getOption(invoke(jconf, "flowExtraHttpHeaders")) },

    isInternalSecureConnectionsEnabled = function() { invoke(jconf, "isInternalSecureConnectionsEnabled") },

    flowDir = function() { ConfBase.getOption(invoke(jconf, "flowDir")) },

    clientIp = function() { ConfBase.getOption(invoke(jconf, "clientIp")) },

    clientIcedDir = function() { ConfBase.getOption(invoke(jconf, "clientIcedDir")) },

    h2oClientLogLevel = function() { invoke(jconf, "h2oClientLogLevel") },

    h2oClientLogDir = function() { ConfBase.getOption(invoke(jconf, "h2oClientLogDir")) },

    clientBasePort = function() { invoke(jconf, "clientBasePort") },

    clientWebPort = function() { invoke(jconf, "clientWebPort") },

    clientVerboseOutput = function() { invoke(jconf, "clientVerboseOutput") },

    clientNetworkMask = function() { ConfBase.getOption(invoke(jconf, "clientNetworkMask")) },

    ignoreSparkPublicDNS = function() { invoke(jconf, "ignoreSparkPublicDNS") },

    clientWebEnabled = function() { invoke(jconf, "clientWebEnabled") },

    clientFlowBaseurlOverride = function() { ConfBase.getOption(invoke(jconf, "clientFlowBaseurlOverride")) },

    clientExtraProperties = function() { ConfBase.getOption(invoke(jconf, "clientExtraProperties")) },

    runsInExternalClusterMode = function() { invoke(jconf, "runsInExternalClusterMode") },

    runsInInternalClusterMode = function() { invoke(jconf, "runsInInternalClusterMode") },

    clientCheckRetryTimeout = function() { invoke(jconf, "clientCheckRetryTimeout") },

    verifySslCertificates = function() { invoke(jconf, "verifySslCertificates") },

#
# Setters
#
    setInternalClusterMode = function() { invoke(jconf, "setInternalClusterMode"); .self },

    setExternalClusterMode = function() { invoke(jconf, "setExternalClusterMode"); .self },

    setCloudName = function(cloudName) { invoke(jconf, "setCloudName", cloudName); .self },

    setNthreads = function(nthreads) { invoke(jconf, "setNthreads", nthreads); .self },

    setReplEnabled = function() { invoke(jconf, "setReplEnabled"); .self },

    setReplDisabled = function() { invoke(jconf, "setReplDisabled"); .self },

    setDefaultNumReplSessions = function(numSessions) { invoke(jconf, "setDefaultNumReplSessions", numSessions); .self },

    setClusterTopologyListenerEnabled = function() { invoke(jconf, "setClusterTopologyListenerEnabled"); .self },

    setClusterTopologyListenerDisabled = function() { invoke(jconf, "setClusterTopologyListenerDisabled"); .self },

    setSparkVersionCheckEnabled = function() { invoke(jconf, "setSparkVersionCheckEnabled"); .self },

    setSparkVersionCheckDisabled = function() { invoke(jconf, "setSparkVersionCheckDisabled"); .self },

    setFailOnUnsupportedSparkParamEnabled = function() { invoke(jconf, "setFailOnUnsupportedSparkParamEnabled"); .self },

    setFailOnUnsupportedSparkParamDisabled = function() { invoke(jconf, "setFailOnUnsupportedSparkParamDisabled"); .self },

    setJks = function(path) { invoke(jconf, "setJks", path); .self },

    setJksPass = function(password) { invoke(jconf, "setJksPass", password); .self },

    setJksAlias = function(alias) { invoke(jconf, "setJksAlias", alias); .self },

    setHashLoginEnabled = function() { invoke(jconf, "setHashLoginEnabled"); .self },

    setHashLoginDisabled = function() { invoke(jconf, "setHashLoginDisabled"); .self },

    setLdapLoginEnabled = function() { invoke(jconf, "setLdapLoginEnabled"); .self },

    setLdapLoginDisabled = function() { invoke(jconf, "setLdapLoginDisabled"); .self },

    setKerberosLoginEnabled = function() { invoke(jconf, "setKerberosLoginEnabled"); .self },

    setKerberosLoginDisabled = function() { invoke(jconf, "setKerberosLoginDisabled"); .self },

    setLoginConf = function(filePath) { invoke(jconf, "setLoginConf", filePath); .self },

    setUserName = function(username) { invoke(jconf, "setUserName", username); .self },

    setPassword = function(password) { invoke(jconf, "setPassword", password); .self },

    setSslConf = function(path) { invoke(jconf, "setSslConf", path); .self },

    setAutoFlowSslEnabled = function() { invoke(jconf, "setAutoFlowSslEnabled"); .self },

    setAutoFlowSslDisabled = function() { invoke(jconf, "setAutoFlowSslDisabled"); .self },

    setH2ONodeLogLevel = function(level) { invoke(jconf, "setH2ONodeLogLevel", level); .self },

    setH2ONodeLogDir = function(dir) { invoke(jconf, "setH2ONodeLogDir", dir); .self },

    setBackendHeartbeatInterval = function(interval) { invoke(jconf, "setBackendHeartbeatInterval", interval); .self },

    setCloudTimeout = function(timeout) { invoke(jconf, "setCloudTimeout", timeout); .self },

    setNodeNetworkMask = function(mask) { invoke(jconf, "setNodeNetworkMask", mask); .self },

    setStacktraceCollectorInterval = function(interval) { invoke(jconf, "setStacktraceCollectorInterval", interval); .self },

    setContextPath = function(contextPath) { invoke(jconf, "setContextPath", contextPath); .self },

    setFlowScalaCellAsyncEnabled = function() { invoke(jconf, "setFlowScalaCellAsyncEnabled"); .self },

    setFlowScalaCellAsyncDisabled = function() { invoke(jconf, "setFlowScalaCellAsyncDisabled"); .self },

    setMaxParallelScalaCellJobs = function(limit) { invoke(jconf, "setMaxParallelScalaCellJobs", limit); .self },

    setInternalPortOffset = function(offset) { invoke(jconf, "setInternalPortOffset", offset); .self },

    setNodeBasePort = function(port) { invoke(jconf, "setNodeBasePort", port); .self },

    setMojoDestroyTimeout = function(timeoutInMilliseconds) { invoke(jconf, "setMojoDestroyTimeout", timeoutInMilliseconds); .self },

    setNodeExtraProperties = function(extraProperties) { invoke(jconf, "setNodeExtraProperties", extraProperties); .self },

    setFlowExtraHttpHeaders = function(headers) { invoke(jconf, "setFlowExtraHttpHeaders", headers); .self },

    setInternalSecureConnectionsEnabled = function() { invoke(jconf, "setInternalSecureConnectionsEnabled"); .self },

    setInternalSecureConnectionsDisabled = function() { invoke(jconf, "setInternalSecureConnectionsDisabled"); .self },

    setFlowDir = function(dir) { invoke(jconf, "setFlowDir", dir); .self },

    setClientIp = function(ip) { invoke(jconf, "setClientIp", ip); .self },

    setClientIcedDir = function(icedDir) { invoke(jconf, "setClientIcedDir", icedDir); .self },

    setH2OClientLogLevel = function(level) { invoke(jconf, "setH2OClientLogLevel", level); .self },

    setH2OClientLogDir = function(dir) { invoke(jconf, "setH2OClientLogDir", dir); .self },

    setClientPortBase = function(basePort) { invoke(jconf, "setClientPortBase", basePort); .self },

    setClientWebPort = function(port) { invoke(jconf, "setClientWebPort", port); .self },

    setClientVerboseEnabled = function() { invoke(jconf, "setClientVerboseEnabled"); .self },

    setClientVerboseDisabled = function() { invoke(jconf, "setClientVerboseDisabled"); .self },

    setClientNetworkMask = function(mask) { invoke(jconf, "setClientNetworkMask", mask); .self },

    setIgnoreSparkPublicDNSEnabled = function() { invoke(jconf, "setIgnoreSparkPublicDNSEnabled"); .self },

    setIgnoreSparkPublicDNSDisabled = function() { invoke(jconf, "setIgnoreSparkPublicDNSDisabled"); .self },

    setClientWebEnabled = function() { invoke(jconf, "setClientWebEnabled"); .self },

    setClientWebDisabled = function() { invoke(jconf, "setClientWebDisabled"); .self },

    setClientFlowBaseurlOverride = function(baseUrl) { invoke(jconf, "setClientFlowBaseurlOverride", baseUrl); .self },

    setClientCheckRetryTimeout = function(timeout) { invoke(jconf, "setClientCheckRetryTimeout", timeout); .self },

    setClientExtraProperties = function(extraProperties) { invoke(jconf, "setClientExtraProperties", extraProperties); .self },

    setVerifySslCertificates = function(verify) { invoke(jconf, "setVerifySslCertificates", verify); .self }
))
