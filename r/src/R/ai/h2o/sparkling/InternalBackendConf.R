#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http {//www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

if (!exists("ConfUtils.getOption", mode = "function")) source(file.path("R", "ConfUtils.R"))

#' @export InternalBackendConf
InternalBackendConf <- setRefClass("InternalBackendConf", methods = list(

#
# Getters
#
    numH2OWorkers = function() { ConfUtils.getOption(invoke(jconf, "numH2OWorkers")) },

    drddMulFactor = function() { invoke(jconf, "drddMulFactor") },

    numRddRetries = function() { invoke(jconf, "numRddRetries") },

    defaultCloudSize = function() { invoke(jconf, "defaultCloudSize") },

    subseqTries = function() { invoke(jconf, "subseqTries") },

    h2oNodeWebEnabled = function() { invoke(jconf, "h2oNodeWebEnabled") },

    nodeIcedDir = function() { ConfUtils.getOption(invoke(jconf, "nodeIcedDir")) },

    hdfsConf = function() { ConfUtils.getOption(invoke(jconf, "hdfsConf")) },

#
# Setters
#
    setNumH2OWorkers = function(numWorkers) { invoke(jconf, "setNumH2OWorkers", numWorkers); .self },

    setDrddMulFactor = function(factor) { invoke(jconf, "setDrddMulFactor", factor); .self },

    setNumRddRetries = function(retries) { invoke(jconf, "setNumRddRetries", retries); .self },

    setDefaultCloudSize = function(defaultClusterSize) { invoke(jconf, "setDefaultCloudSize", defaultClusterSize); .self },

    setSubseqTries = function(subseqTriesNum) { invoke(jconf, "setSubseqTries", subseqTriesNum); .self },

    setH2ONodeWebEnabled = function() { invoke(jconf, "setH2ONodeWebEnabled"); .self },

    setH2ONodeWebDisabled = function() { invoke(jconf, "setH2ONodeWebDisabled"); .self },

    setNodeIcedDir = function(dir) { invoke(jconf, "setNodeIcedDir", dir); .self },

    setHdfsConf = function(hdfsConf) { invoke(jconf, "setHdfsConf", hdfsConf); .self }
))
