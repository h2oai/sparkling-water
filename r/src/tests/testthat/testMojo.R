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

context("Test MOJO predictions")

config <- spark_config()
config <- c(config, list(
  "spark.hadoop.yarn.timeline-service.enabled" = "false",
  "spark.ext.h2o.external.cluster.size" = "1",
  "spark.ext.h2o.backend.cluster.mode" = Sys.getenv("spark.ext.h2o.backend.cluster.mode"),
  "spark.ext.h2o.client.ip" = Sys.getenv("H2O_CLIENT_IP"),
  "sparklyr.gateway.connect.timeout" = 240,
  "sparklyr.gateway.start.timeout" = 240,
  "sparklyr.backend.timeout" = 240,
  "spark.ext.h2o.external.start.mode" = "auto",
  "spark.ext.h2o.external.disable.version.check" = "true"
))

locate <- function(fileName) {
  firstPath <- file.path("/home/0xdiag/", fileName)
  if (file.exists(firstPath)) {
    firstPath
  } else {
    normalizePath(file.path("../../../../examples/", fileName))
  }
}


test_that("test MOJO predictions", {
  sc <- spark_connect(master = "local[*]", config = config)
  path <- paste0("file://", locate("smalldata/prostate/prostate.csv"))
  dataset <- spark_read_csv(sc, path = path, infer_schema = TRUE, header = TRUE)
  # Try loading the Mojo and prediction on it without starting H2O Context
  mojo <- H2OMOJOModel.createFromMojo(paste0("file://", normalizePath("../../../../ml/src/test/resources/binom_model_prostate.mojo")))
  sdf <- mojo$transform(dataset)
  data <- dplyr::collect(mojo$transform(sdf))
  expect_equal(colnames(data), c("ID", "CAPSULE", "AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON", "prediction"))
})

test_that("test getDomainValues", {
  sc <- spark_connect(master = "local[*]", config = config)
  model <- H2OMOJOModel.createFromMojo(paste0("file://", normalizePath("../../../../ml/src/test/resources/binom_model_prostate.mojo")))
  domainValues <- model$getDomainValues()
  expect_true(is.null(domainValues[["DPROS"]]))
  expect_true(is.null(domainValues[["DCAPS"]]))
  expect_true(is.null(domainValues[["VOL"]]))
  expect_true(is.null(domainValues[["AGE"]]))
  expect_true(is.null(domainValues[["PSA"]]))
  expect_equal(domainValues[["capsule"]][[1]], "0")
  expect_equal(domainValues[["capsule"]][[2]], "1")
  expect_true(is.null(domainValues[["RACE"]]))
  expect_true(is.null(domainValues[["ID"]]))
})


test_that("test MOJO predictions on unseen categoricals", {
  sc <- spark_connect(master = "local[*]", config = config)
  path <- paste0("file://", normalizePath("../../../../ml/src/test/resources/deep_learning_airlines_categoricals.zip"))
  settings <- H2OMOJOSettings(convertUnknownCategoricalLevelsToNa = TRUE)
  mojo <- H2OMOJOModel.createFromMojo(path, settings)

  df <- as.data.frame(t(c(5.1, 3.5, 1.4, 0.2, "Missing_categorical")))
  colnames(df) <- c("sepal_len", "sepal_wid", "petal_len", "petal_wid", "class")
  sdf <- copy_to(sc, df, overwrite = TRUE)

  data <- dplyr::collect(mojo$transform(sdf))

  expect_equal(as.character(dplyr::select(data, class)), "Missing_categorical")
  expect_equal(as.double(dplyr::select(data, petal_len)), 1.4)
  expect_equal(as.double(dplyr::select(data, petal_wid)), 0.2)
  expect_equal(as.double(dplyr::select(data, sepal_len)), 5.1)
  expect_equal(as.double(dplyr::select(data, sepal_wid)), 3.5)
  expect_equal(as.double(dplyr::select(data, prediction)), 5.240174068202646)
})
