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

master <- Sys.getenv("KUBERNETES_MASTER")
registryId <- Sys.getenv("REGISTRY_ID")
version <- Sys.getenv("SW_VERSION")
sparkHome <- Sys.getenv("SPARK_HOME")
sparkVersion <- Sys.getenv("SPARK_VERSION")
config <- spark_config_kubernetes(master = master,
                                  image = paste0(registryId, ".dkr.ecr.us-east-2.amazonaws.com/sw_kubernetes_repo/sparkling-water:r-", version),
                                  driver = "driver-r",
                                  account = "default",
                                  executors = 3,
                                  version = sparkVersion,
                                  ports = c(8880, 8881, 4040, 54323))
config["spark.home"] <- Sys.getenv("SPARK_HOME")
config["sparklyr.shell.files"] <- "/opt/sparkling-water/tests/initTest.R"
spark_submit(master = master, file = "/opt/sparkling-water/tests/initTest.R", config = config, spark_home = sparkHome)
