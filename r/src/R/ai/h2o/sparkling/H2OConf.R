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

getOption <- function(option) {
  if (invoke(option, "isDefined")) {
    invoke(option, "get")
  } else {
    NA_character_
  }
}

#' @export H2OConf
H2OConf <- setRefClass("H2OConf", fields = list(jconf = "ANY"), methods = list(
  initialize = function(spark) {
    .self$jconf <- invoke_new(spark, "org.apache.spark.h2o.H2OConf", spark_context(spark))
  },

  userName = function() { getOption(invoke(jconf, "userName")) },
  setUserName = function(value) { invoke(jconf, "setUserName", value); .self },

  password = function() { getOption(invoke(jconf, "password")) },
  setPassword = function(value) { invoke(jconf, "setPassword", value); .self },

  externalHadoopExecutable = function() {invoke(jconf, "externalHadoopExecutable")},
  setExternalHadoopExecutable = function(value) {invoke(jconf, "setExternalHadoopExecutable", value); .self },

  isInternalSecureConnectionsEnabled = function() { invoke(jconf, "isInternalSecureConnectionsEnabled") },
  setInternalSecureConnectionsEnabled = function() { invoke(jconf, "setInternalSecureConnectionsEnabled"); .self },
  setInternalSecureConnectionsDisabled = function() { invoke(jconf, "setInternalSecureConnectionsDisabled"); .self },

  sslConf = function() { getOption(invoke(jconf, "sslConf")) },
  setSslConf = function(value) { invoke(jconf, "setSslConf", value); .self },

  verifySslCertificates = function() { invoke(jconf, "verifySslCertificates") },
  setVerifySslCertificates = function(value) {invoke(jconf, "setVerifySslCertificates", value); .self },

  jks = function() { getOption(invoke(jconf, "jks")) },
  setJks = function(value) {invoke(jconf, "setJks", value); .self },

  jksPass = function() { getOption(invoke(jconf, "jksPass")) },
  setJksPass = function(value) {invoke(jconf, "setJksPass", value); .self },

  autoFlowSsl = function() { getOption(invoke(jconf, "autoFlowSsl")) },
  setAutoFlowSslEnabled = function() {invoke(jconf, "setAutoFlowSslEnabled"); .self },
  setAutoFlowSslDisabled = function() {invoke(jconf, "setAutoFlowSslDisabled"); .self }
))
