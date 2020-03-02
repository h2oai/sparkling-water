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

#' @export ExternalBackendConf
ExternalBackendConf <- setRefClass("ExternalBackendConf", contains = "ConfBase", methods = list(

#
# Getters
#
    h2oCluster = function() { ConfBase.getOption(invoke(jconf, "h2oCluster")) },

    h2oClusterHost = function() { ConfBase.getOption(invoke(jconf, "h2oClusterHost")) },

    h2oClusterPort = function() { ConfBase.getOption(invoke(jconf, "h2oClusterPort")) },

    clusterSize = function() { ConfBase.getOption(invoke(jconf, "clusterSize")) },

    clusterStartTimeout = function() { invoke(jconf, "clusterStartTimeout") },

    clusterInfoFile = function() { ConfBase.getOption(invoke(jconf, "clusterInfoFile")) },

    mapperXmx = function() { invoke(jconf, "mapperXmx") },

    HDFSOutputDir = function() { ConfBase.getOption(invoke(jconf, "HDFSOutputDir")) },

    isAutoClusterStartUsed = function() { invoke(jconf, "isAutoClusterStartUsed") },

    isManualClusterStartUsed = function() { invoke(jconf, "isManualClusterStartUsed") },

    clusterStartMode = function() { invoke(jconf, "clusterStartMode") },

    h2oDriverPath = function() { ConfBase.getOption(invoke(jconf, "h2oDriverPath")) },

    YARNQueue = function() { ConfBase.getOption(invoke(jconf, "YARNQueue")) },

    isKillOnUnhealthyClusterEnabled = function() { invoke(jconf, "isKillOnUnhealthyClusterEnabled") },

    kerberosPrincipal = function() { ConfBase.getOption(invoke(jconf, "kerberosPrincipal")) },

    kerberosKeytab = function() { ConfBase.getOption(invoke(jconf, "kerberosKeytab")) },

    runAsUser = function() { ConfBase.getOption(invoke(jconf, "runAsUser")) },

    externalH2ODriverIf = function() { ConfBase.getOption(invoke(jconf, "externalH2ODriverIf")) },

    externalH2ODriverPort = function() { ConfBase.getOption(invoke(jconf, "externalH2ODriverPort")) },

    externalH2ODriverPortRange = function() { ConfBase.getOption(invoke(jconf, "externalH2ODriverPortRange")) },

    externalExtraMemoryPercent = function() { invoke(jconf, "externalExtraMemoryPercent") },

    externalBackendStopTimeout = function() { invoke(jconf, "externalBackendStopTimeout") },

    externalHadoopExecutable = function() { invoke(jconf, "externalHadoopExecutable") },

    externalExtraJars = function() { ConfBase.getOption(invoke(jconf, "externalExtraJars")) },

    externalCommunicationCompression = function() { invoke(jconf, "externalCommunicationCompression") },

#
# Setters
#
    setH2OCluster = function(ip, port) { invoke(jconf, "setH2OCluster", ip, port); .self },

    setClusterSize = function(clusterSize) { invoke(jconf, "setClusterSize", clusterSize); .self },

    setClusterStartTimeout = function(clusterStartTimeout) { invoke(jconf, "setClusterStartTimeout", clusterStartTimeout); .self },

    setClusterConfigFile = function(path) { invoke(jconf, "setClusterConfigFile", path); .self },

    setMapperXmx = function(mem) { invoke(jconf, "setMapperXmx", mem); .self },

    setHDFSOutputDir = function(dir) { invoke(jconf, "setHDFSOutputDir", dir); .self },

    useAutoClusterStart = function() { invoke(jconf, "useAutoClusterStart"); .self },

    useManualClusterStart = function() { invoke(jconf, "useManualClusterStart"); .self },

    setH2ODriverPath = function(path) { invoke(jconf, "setH2ODriverPath(", path); .self },

    setYARNQueue = function(queueName) { invoke(jconf, "setYARNQueue", queueName); .self },

    setKillOnUnhealthyClusterEnabled = function() { invoke(jconf, "setKillOnUnhealthyClusterEnabled"); .self },

    setKillOnUnhealthyClusterDisabled = function() { invoke(jconf, "setKillOnUnhealthyClusterDisabled"); .self },

    setKerberosPrincipal = function(principal) { invoke(jconf, "setKerberosPrincipal", principal); .self },

    setKerberosKeytab = function(path) { invoke(jconf, "setKerberosKeytab", path); .self },

    setRunAsUser = function(user) { invoke(jconf, "setRunAsUser", user); .self },

    setExternalH2ODriverIf = function(iface) { invoke(jconf, "setExternalH2ODriverIf", iface); .self },

    setExternalH2ODriverPort = function(port) { invoke(jconf, "setExternalH2ODriverPort", port); .self },

    setExternalH2ODriverPortRange = function(portrange) { invoke(jconf, "setExternalH2ODriverPortRange", portrange); .self },

    setExternalExtraMemoryPercent = function(memoryPercent) { invoke(jconf, "setExternalExtraMemoryPercent", memoryPercent); .self },

    setExternalBackendStopTimeout = function(timeout) { invoke(jconf, "setExternalBackendStopTimeout", timeout); .self },

    setExternalHadoopExecutable = function(executable) { invoke(jconf, "setExternalHadoopExecutable", executable); .self },

    setExternalExtraJars = function(paths) { invoke(jconf, "setExternalExtraJars", paths); .self },

    setExternalCommunicationCompression = function(compression) { invoke(jconf, "setExternalCommunicationCompression", compression); .self }
))
