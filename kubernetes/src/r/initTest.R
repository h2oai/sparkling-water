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
library(sparklyr)
library(rsparkling)
library(testthat)
master <- Sys.getenv("KUBERNETES_MASTER")
registryId <- Sys.getenv("REGISTRY_ID")
version <- Sys.getenv("SW_VERSION")
sparkHome <- Sys.getenv("SPARK_HOME")
extraOptions <- Sys.getenv("EXTRA_OPTIONS")
if (extraOptions == "") {
  extraOptionsParsed <- NULL
} else {
  extraOptionsParsed <- unlist(strsplit(extraOptions," "))
}

config <- spark_config_kubernetes(master = master,
                                 image = paste0(registryId, ".dkr.ecr.us-east-2.amazonaws.com/sw_kubernetes_repo/sparkling-water:r-", version),
                                 account = "default",
                                 driver ="driver-r",
                                 executors = 2,
                                 conf = extraOptionsParsed,
                                 ports = c(8880, 8881, 4040, 54321))
config["spark.home"] <-  sparkHome
sc <- spark_connect(config = config, spark_home = sparkHome)
hc <- H2OContext.getOrCreate()
expect_equal(length(invoke(hc$jhc, "getH2ONodes")), 2)

# Test conversions
df <- as.data.frame(t(c(1, 2, 3, 4, "A")))
sdf <- copy_to(sc, df, overwrite = TRUE)
hc <- H2OContext.getOrCreate()
hf <- hc$asH2OFrame(sdf)
sdf2 <- hc$asSparkFrame(hf)
expect_equal(sdf_nrow(sdf2), nrow(hf))
expect_equal(sdf_ncol(sdf2), ncol(hf))
expect_equal(colnames(sdf2), colnames(hf))

spark_disconnect(sc)
