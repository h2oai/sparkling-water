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
.rsparklingenv <- new.env(parent = emptyenv())
.rsparklingenv$isConnected <- FALSE

getClientConnectedField <- function(jhc) {
  context <- sparklyr::invoke(jhc, "getClass")
  field <- sparklyr::invoke(context, "getDeclaredField", "clientConnected")
  sparklyr::invoke(field, "setAccessible", TRUE)
  field
}

isClientConnected <- function(jhc) {
  field <- getClientConnectedField(jhc)
  sparklyr::invoke(field, "get", jhc)
}

setClientConnected <- function(jhc) {
  field <- getClientConnectedField(jhc)
  sparklyr::invoke(field, "set", jhc, TRUE)
}

#' @export H2OContext.getOrCreate
H2OContext.getOrCreate <- function(sc = NULL, conf = NULL) {

  if (!is.null(sc) && typeof(sc) == "list") {
    print("Method getOrCreate with the sc argument is deprecated. Please use either just getOrCreate() or if you need
      to pass extra H2OConf, use getOrCreate(conf). The sc argument will be removed in release 3.32.")
  }

  if (!is.null(sc) && typeof(sc) == "S4") {
    # it means user is passing conf as first argument
    conf <- sc
  } else if (is.null(conf)) {
    conf <- H2OConf()
  }
  if (conf$runsInExternalClusterMode()) {
    conf$set("spark.ext.h2o.rest.api.based.client", "true")
  }

  sc <- spark_connection_find()[[1]]
  jhc <- invoke_static(sc, "org.apache.spark.h2o.H2OContext", "getOrCreate", conf$jconf)
  hc <- H2OContext(jhc)
  returnedConf <- invoke(jhc, "getConf")
  # Because of checks in Sparkling Water, we are sure context path starts with one slash
  contextPathWithSlash <- invoke(returnedConf, "get", "spark.ext.h2o.context.path", "")
  contextPath <- substring(contextPathWithSlash, 2, nchar(contextPathWithSlash))
  ip <- invoke(jhc, "h2oLocalClientIp")
  port <- invoke(jhc, "h2oLocalClientPort")
  schema <- invoke(returnedConf, "getScheme")
  insecure <- conf$verifySslCertificates() == FALSE
  https <- schema == "https"
  if (!isClientConnected(jhc) || !.rsparklingenv$isConnected) {
    if (contextPath == "") {
        contextPath <- NA_character_
    }
    h2o.init(strict_version_check = FALSE, https=https, insecure=insecure, ip = ip, port = port, context_path = contextPath, startH2O = F, username = conf$userName(), password = conf$password())
    setClientConnected(jhc)
    .rsparklingenv$isConnected <- TRUE
    print(hc)
  }
  hc
}

#' @export H2OContext
H2OContext <- setRefClass("H2OContext", fields = list(jhc = "ANY"), methods = list(
  initialize = function(jhc) {
    .self$jhc <- jhc
  },
  openFlow = function() {
    flowURL <- invoke(.self$jhc, "flowURL")
    browseURL(flowURL)
  },
  asH2OFrame = function(sparkFrame, h2oFrameName = NULL) {
    # Ensure we are dealing with a Spark DataFrame (might be e.g. a tbl)
    sparkFrame <- spark_dataframe(sparkFrame)
    key <- if (is.null(h2oFrameName)) {
      invoke(.self$jhc, "asH2OFrameKeyString", sparkFrame)
    } else {
      invoke(.self$jhc, "asH2OFrameKeyString", sparkFrame, h2oFrameName)
    }

    h2o.getFrame(key)
  },
  asSparkFrame = function(h2oFrame, copyMetaData = TRUE) {
    sparkFrame <- invoke(.self$jhc, "asDataFrame", h2o.getId(h2oFrame), copyMetaData)
    # Register returned spark_jobj as a table for dplyr
    sdf_register(sparkFrame)
  }
))
