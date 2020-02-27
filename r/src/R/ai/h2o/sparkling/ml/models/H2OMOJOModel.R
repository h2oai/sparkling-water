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

source(file.path("R", "H2OMOJOModelBase.R"))

H2OMOJOModel.createFromMojo <- function(pathToMojo, settings = H2OMOJOSettings.default()) {
  sc <- spark_connection_find()[[1]]
  jmojo <- invoke_static(sc, "ai.h2o.sparkling.ml.models.H2OMOJOModel", "createFromMojo", pathToMojo, settings$toJavaObject())
  className <- invoke(invoke(jmojo, "getClass"), "getSimpleName")
  if (className == "H2OTreeBasedSupervisedMOJOModel") {
    H2OTreeBasedSupervisedMOJOModel(jmojo)
  } else if (className == "H2OSupervisedMOJOModel") {
    H2OSupervisedMOJOModel(jmojo)
  } else if (className == "H2OUnsupervisedMOJOModel") {
    H2OUnsupervisedMOJOModel(jmojo)
  } else {
    H2OMOJOModel(jmojo)
  }
}

#' @export H2OMOJOModel
H2OMOJOModel <- setRefClass("H2OMOJOModel", contains = ("H2OMOJOModelBase"), methods = list(
  getModelDetails = function() {
    invoke(.self$jmojo, "getModelDetails")
  },
  getDomainValues = function() {
    invoke(.self$jmojo, "getDomainValues")
  },
  getDomainValuesForCol = function(col) {
    invoke(.self$jmojo, "getDomainValuesForCol", col)
  }
))

#' @export H2OSupervisedMOJOModel
H2OSupervisedMOJOModel <- setRefClass("H2OSupervisedMOJOModel", contains = ("H2OMOJOModel"), methods = list(
  getOffsetCol = function() {
    invoke(.self$jmojo, "getOffsetCol")
  }
))

#' @export H2OTreeBasedSupervisedMOJOModel
H2OTreeBasedSupervisedMOJOModel <- setRefClass("H2OTreeBasedSupervisedMOJOModel", contains = ("H2OSupervisedMOJOModel"), methods = list(
  getNtrees = function() {
    invoke(.self$jmojo, "getNtrees")
  }
))

#' @export H2OUnsupervisedMOJOModel
H2OUnsupervisedMOJOModel <- setRefClass("H2OUnupervisedMOJOModel", contains = ("H2OMOJOModel"), fields = list(jmojo = "ANY"), methods = list(
))
