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

context("Test MOJO pipeline")

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
  normalizePath(file.path("../../../../../ml/src/test/resources/", fileName))
}

test_that("test MOJO contribution (SHAP) values", {
  mojo_path <- paste0("file://", locate("daiMojoShapley/pipeline.mojo"))
  data_path <- paste0("file://", locate("daiMojoShapley/example.csv"))
  dataset <- spark_read_csv(sc, path = data_path, infer_schema = TRUE, header = TRUE)
  # request contributions
  settings <- H2OMOJOSettings(withContributions = TRUE)
  mojo <- H2OMOJOPipelineModel.createFromMojo(mojo_path, settings)
  mojoOutput <- mojo$transform(dataset)

  flattenedContributions <- sdf_unnest_wider(mojoOutput, "contributions")
  expect_equal(colnames(flattenedContributions), c(
    "sepal_len",                         # input feature
    "sepal_wid",                         # input feature
    "petal_len",                         # input feature
    "petal_wid",                         # input feature
    "prediction",                        # output prediction
    "contrib_sepal_len.Iris-setosa",     # output contributions
    "contrib_sepal_wid.Iris-setosa",     # output contributions
    "contrib_petal_len.Iris-setosa",     # output contributions
    "contrib_petal_wid.Iris-setosa",     # output contributions
    "contrib_bias.Iris-setosa",          # output contributions
    "contrib_sepal_len.Iris-versicolor", # output contributions
    "contrib_sepal_wid.Iris-versicolor", # output contributions
    "contrib_petal_len.Iris-versicolor", # output contributions
    "contrib_petal_wid.Iris-versicolor", # output contributions
    "contrib_bias.Iris-versicolor",      # output contributions
    "contrib_sepal_len.Iris-virginica",  # output contributions
    "contrib_sepal_wid.Iris-virginica",  # output contributions
    "contrib_petal_len.Iris-virginica",  # output contributions
    "contrib_petal_wid.Iris-virginica",  # output contributions
    "contrib_bias.Iris-virginica"))      # output contributions
})

spark_disconnect(sc)
