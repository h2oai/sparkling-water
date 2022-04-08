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

context("Test metrics calculation")

config <- spark_config()
config <- c(config, list(
  "spark.hadoop.yarn.timeline-service.enabled" = "false",
  "spark.ext.h2o.external.cluster.size" = "1",
  "spark.ext.h2o.backend.cluster.mode" = Sys.getenv("spark.ext.h2o.backend.cluster.mode"),
  "sparklyr.connect.enablehivesupport" = FALSE,
  "sparklyr.gateway.connect.timeout" = 240,
  "sparklyr.gateway.start.timeout" = 240,
  "sparklyr.backend.timeout" = 240,
  "sparklyr.log.console" = TRUE,
  "spark.ext.h2o.external.start.mode" = "auto",
  "spark.ext.h2o.external.disable.version.check" = "true",
  "sparklyr.gateway.port" = 55555,
  "sparklyr.connect.timeout" = 60 * 5,
  "spark.master" = "local[*]"
))

for (i in 1:4) {
  tryCatch(
    {
    sc <- spark_connect(master = "local[*]", config = config)
  }, error = function(e) { }
  )
}

locate <- function(fileName) {
  normalizePath(file.path("../../../../../examples/", fileName))
}

test_that("test training metrics", {
  model <- H2OMOJOModel.createFromMojo(paste0("file://", normalizePath("../../../../../ml/src/test/resources/binom_model_prostate.mojo")))
  metrics <- model$getTrainingMetrics()
  expect_equal(as.character(metrics[["AUC"]]), "0.896878869021911")
  expect_equal(length(metrics), 10)
})

test_that("test training metrics object", {
  model <- H2OMOJOModel.createFromMojo(paste0("file://", normalizePath("../../../../../ml/src/test/resources/binom_model_prostate.mojo")))
  metrics <- model$getTrainingMetricsObject()
  aucValue <- metrics$getAUC()
  scoringTime <- metrics$getScoringTime()

  thresholdsAndScores <- metrics$getThresholdsAndMetricScores()
  thresholdsAndScoresFrame <- dplyr::tally(thresholdsAndScores)
  thresholdsAndScoresCount <- as.double(dplyr::collect(thresholdsAndScoresFrame)[[1]])

  gainsLiftTable <- metrics$getGainsLiftTable()
  gainsLiftTableFrame <- dplyr::tally(gainsLiftTable)
  gainsLiftTableCount <- as.double(dplyr::collect(gainsLiftTableFrame)[[1]])

  expect_equal(as.character(aucValue), "0.896878869021911")
  expect_true(scoringTime > 0)
  expect_true(thresholdsAndScoresCount > 0)
  expect_true(gainsLiftTableCount > 0)
})

test_that("test null cross validation metrics object", {
  model <- H2OMOJOModel.createFromMojo(paste0("file://", normalizePath("../../../../../ml/src/test/resources/binom_model_prostate.mojo")))
  cvObject <- model$getCrossValidationMetricsObject()
  expect_true(is.null(cvObject))
})

test_that("test current metrics", {
  model <- H2OMOJOModel.createFromMojo(paste0("file://", normalizePath("../../../../../ml/src/test/resources/binom_model_prostate.mojo")))
  metrics <- model$getCurrentMetrics()
  expect_equal(metrics, model$getTrainingMetrics())
})

test_that("test calculation of regression metrics", {
  path <- paste0("file://", locate("smalldata/prostate/prostate.csv"))
  dataset <- spark_read_csv(sc, path = path, infer_schema = TRUE, header = TRUE)
  model <- H2OMOJOModel.createFromMojo(paste0("file://", normalizePath("../../../../../ml/src/test/resources/regre_model_prostate.mojo")))
  predictions <- model$transform(dataset)

  metrics <- H2ORegressionMetrics.calculate(predictions,  labelCol = "CAPSULE")

  mae <- metrics$getMAE()
  rmsle <- metrics$getRMSLE()

  expect_true(mae > 0.0)
  expect_true(rmsle > 0.0)
})

test_that("test calculation of binomial metrics", {
  path <- paste0("file://", locate("smalldata/prostate/prostate.csv"))
  dataset <- spark_read_csv(sc, path = path, infer_schema = TRUE, header = TRUE)
  model <- H2OMOJOModel.createFromMojo(paste0("file://", normalizePath("../../../../../ml/src/test/resources/binom_model_prostate.mojo")))
  predictions <- model$transform(dataset)
  domainValues <- model$getDomainValues()

  metrics <- H2OBinomialMetrics.calculate(predictions, domainValues[["capsule"]], labelCol = "CAPSULE")

  aucValue <- metrics$getAUC()
  scoringTime <- metrics$getScoringTime()

  thresholdsAndScores <- metrics$getThresholdsAndMetricScores()
  thresholdsAndScoresFrame <- dplyr::tally(thresholdsAndScores)
  thresholdsAndScoresCount <- as.double(dplyr::collect(thresholdsAndScoresFrame)[[1]])

  expect_true(aucValue > 0.6)
  expect_true(scoringTime > 0)
  expect_true(thresholdsAndScoresCount > 0)
})

test_that("test calculation of multinomial metrics", {
  path <- paste0("file://", locate("smalldata/iris/iris_wheader.csv"))
  dataset <- spark_read_csv(sc, path = path, infer_schema = TRUE, header = TRUE)
  model <- H2OMOJOModel.createFromMojo(paste0("file://", normalizePath("../../../../../ml/src/test/resources/multi_model_iris.mojo")))
  predictions <- model$transform(dataset)
  domainValues <- model$getDomainValues()

  metrics <- H2OMultinomialMetrics.calculate(predictions, domainValues[["class"]], labelCol = "class")

  aucValue <- metrics$getAUC()
  scoringTime <- metrics$getScoringTime()

  confusionMatrix <- metrics$getConfusionMatrix()
  confusionMatrixFrame <- dplyr::tally(confusionMatrix)
  confusionMatrixCount <- as.double(dplyr::collect(confusionMatrixFrame)[[1]])

  expect_true(aucValue > 0.6)
  expect_true(scoringTime > 0)
  expect_true(confusionMatrixCount > 0)
})

spark_disconnect(sc)
