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

source(file.path("R", "H2ORegressionMetricsBase.R"))

#' @export rsparkling.H2ORegressionMetricsBase
rsparkling.H2ORegressionMetrics <- setRefClass("rsparkling.H2ORegressionMetrics", contains = ("rsparkling.H2ORegressionMetricsBase"))

H2ORegressionMetrics.calculate <- function(dataFrame,
                                           predictionCol = "detailed_prediction",
                                           labelCol = "label",
                                           weightCol = NULL,
                                           offsetCol = NULL) {
    sc <- spark_connection_find()[[1]]
    javaMetrics <- invoke_static(sc,
                                 "ai.h2o.sparkling.ml.metrics.H2ORegressionMetrics",
                                 "calculate",
                                 dataFrame,
                                 predictionCol,
                                 labelCol,
                                 weightCol,
                                 offsetCol)
    rsparkling.H2ORegressionMetrics(javaMetrics)
}
