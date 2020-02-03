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
  password = function() { getOption(invoke(jconf, "password")) },
  setUserName = function(value) { invoke(jconf, "setUserName", value) },
  setPassword = function(value) { invoke(jconf, "setPassword", value) },
  externalHadoopExecutable = function() {invoke(jconf, "externalHadoopExecutable")},
  setExternalHadoopExecutable = function(value) {invoke(jconf, "setExternalHadoopExecutable", value)}
))
