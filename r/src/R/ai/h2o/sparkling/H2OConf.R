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

if (!exists("SharedBackendConf")) source(file.path("R", "SharedBackendConf.R"))
if (!exists("ExternalBackendConf")) source(file.path("R", "ExternalBackendConf.R"))
if (!exists("InternalBackendConf")) source(file.path("R", "InternalBackendConf.R"))

#' @export H2OConf
H2OConf <- setRefClass("H2OConf", fields = list(jconf = "ANY"),
                       contains = c("SharedBackendConf", "ExternalBackendConf", "InternalBackendConf"), methods = list(
    initialize = function(spark = NULL) {
        if (!is.null(spark)) {
            print("Constructor H2OConf(spark) with the spark argument is deprecated. Please use just H2OConf(). The argument will be removed in release 3.32.")
        }
        sc <- spark_connection_find()[[1]]
        .self$jconf <- invoke_new(sc, "ai.h2o.sparkling.H2OConf")
    },

    set = function(option, value) { invoke(jconf, "set", option, value); .self },

    get = function(option) { invoke(jconf, "get", option) }
))
