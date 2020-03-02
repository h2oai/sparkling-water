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

ConfBase.getOption <- function(option) {
    if (invoke(option, "isDefined")) {
        invoke(option, "get")
    } else {
        NA_character_
    }
}

#' @export ConfBase
ConfBase <- setRefClass("ConfBase", fields = list(jmojo = "ANY"), methods = list(
  initialize = function(spark = NULL) {
    if (!is.null(spark)) {
        print("Constructor H2OConf(spark) with the spark argument is deprecated. Please use just H2OConf(). The argument will be removed in release 3.32.")
    }
    sc <- spark_connection_find()[[1]]
    .self$jconf <- invoke_new(sc, "org.apache.spark.h2o.H2OConf")
  },

  set = function(option, value) {invoke(jconf, "set", option, value); .self },
))
