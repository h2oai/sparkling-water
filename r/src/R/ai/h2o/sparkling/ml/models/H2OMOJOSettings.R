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

H2OMOJOSettings.default <- function() {
  H2OMOJOSettings()
}

#' @export H2OMOJOSettings
H2OMOJOSettings <- setRefClass("H2OMOJOSettings",
                               fields = list(predictionCol = "character",
                                             detailedPredictionCol = "character",
                                             withDetailedPredictionCol = "logical",
                                             convertUnknownCategoricalLevelsToNa = "logical",
                                             convertInvalidNumbersToNa = "logical",
                                             namedMojoOutputColumns = "logical"),
                               methods = list(
                                 initialize = function(predictionCol = "prediction",
                                                       detailedPredictionCol = "detailed_prediction",
                                                       withDetailedPredictionCol = FALSE,
                                                       convertUnknownCategoricalLevelsToNa = FALSE,
                                                       convertInvalidNumbersToNa = FALSE,
                                                       namedMojoOutputColumns = TRUE) {
                                   .self$predictionCol <- predictionCol
                                   .self$detailedPredictionCol <- detailedPredictionCol
                                   .self$withDetailedPredictionCol <- withDetailedPredictionCol
                                   .self$convertUnknownCategoricalLevelsToNa <- convertUnknownCategoricalLevelsToNa
                                   .self$convertInvalidNumbersToNa <- convertInvalidNumbersToNa
                                   .self$namedMojoOutputColumns <- namedMojoOutputColumns
                                 },
                                 toJavaObject = function() {
                                   sc <- spark_connection_find()[[1]]
                                   invoke_new(sc, "ai.h2o.sparkling.ml.models.H2OMOJOSettings",
                                              .self$predictionCol,
                                              .self$detailedPredictionCol,
                                              .self$withDetailedPredictionCol,
                                              .self$convertUnknownCategoricalLevelsToNa,
                                              .self$convertInvalidNumbersToNa,
                                              .self$namedMojoOutputColumns)
                                 }
                               ))
