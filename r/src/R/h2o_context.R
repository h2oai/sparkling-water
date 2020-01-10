#' Get or create the H2OContext.
#'
#' @param sc Object of type \code{spark_connection} or \code{spark_jobj}.
#' @export
h2o_context <- function(sc, conf = NULL, username = NA_character_, password = NA_character_) {
  UseMethod("h2o_context")
}

getClientConnectedField <- function(hc) {
  child <- sparklyr::invoke(hc, "getClass")
  context <- sparklyr::invoke(child, "getSuperclass")
  field <- sparklyr::invoke(context, "getDeclaredField", "clientConnected")
  sparklyr::invoke(field, "setAccessible", TRUE)
  field
}

isClientConnected <- function(hc) {
  field <- getClientConnectedField(hc)
  sparklyr::invoke(field, "get", hc)
}

setClientConnected <- function(hc) {
  field <- getClientConnectedField(hc)
  sparklyr::invoke(field, "set", hc, TRUE)
}

#' @export
h2o_context.spark_connection <- function(sc, conf = NULL, username = NA_character_, password = NA_character_) {
  if (is.null(conf)) {
    conf <- H2OConf(sc)
  }
  if (!is.na(username)) {
    print("Providing username via username parameter on H2OContext is deprecated. Please use setUserName H2OConf object.")
    conf$setUserName(username)
  }

  if (!is.na(password)) {
    print("Providing password via password parameter on H2OContext is deprecated. Please use setUserName H2OConf object.")
    conf$setPassword(password)
  }

  hc <- invoke_static(sc, "org.apache.spark.h2o.H2OContext", "getOrCreate", spark_context(sc), conf$jconf)
  returnedConf <- invoke(hc, "getConf")
  # Because of checks in Sparkling Water, we are sure context path starts with one slash
  context_path_with_slash <- invoke(returnedConf, "get", "spark.ext.h2o.context.path",  "")
  context_path <- substring(context_path_with_slash, 2, nchar(context_path_with_slash))
  ip <- invoke(hc, "h2oLocalClientIp")
  port <- invoke(hc, "h2oLocalClientPort")
  if (!isClientConnected(hc)){
    if (context_path == "") {
      invisible(capture.output(h2o.init(strict_version_check = FALSE, ip = ip, port = port, startH2O=F, username = conf$userName(), password = conf$password())))
    } else {
      invisible(capture.output(h2o.init(strict_version_check = FALSE, ip = ip, port = port, context_path = context_path, startH2O=F, username = conf$userName(), password = conf$password())))
    }
    setClientConnected(hc)
  }
  hc
}

#' @export
h2o_context.spark_jobj <- function(sc, conf = NULL, username = NA_character_, password = NA_character_) {
  h2o_context.spark_connection(spark_connection(sc), conf=conf, username = username, password = password)
}

#' Open the H2O Flow UI in a browser
#'
#' @param sc Object of type \code{spark_connection}.
#' @export
h2o_flow <- function(sc) {
  flowURL <- invoke(h2o_context(sc), "flowURL")
  browseURL(flowURL)
}

#' Convert a Spark DataFrame to an H2O Frame
#'
#' @param sc Object of type \code{spark_connection}.
#' @param frame A \code{spark_dataframe}.
#' @param name The name of the H2OFrame.
#' @export
as_h2o_frame <- function(sc, frame, name=NULL) {
  # Ensure we are dealing with a Spark DataFrame (might be e.g. a tbl)
  frame <- spark_dataframe(frame)
  
  # Convert the Spark DataFrame to an H2OFrame
  hc <- h2o_context(sc)
  jhf <- if(is.null(name)) {
    invoke(hc, "asH2OFrame", frame)
  } else {
    invoke(hc, "asH2OFrame", frame, name)
  }

  key <- invoke(invoke(jhf, "key"), "toString")
  h2o.getFrame(key)
}

#' Convert an H2O Frame to a Spark DataFrame
#'
#' @param sc Object of type \code{spark_connection}.
#' @param frame An \code{H2OFrame}.
#' @param name The name to assign the data frame in Spark.
#' @export
as_spark_dataframe <- function(sc, frame, name = paste(deparse(substitute(sc)), collapse="")) {
  hc <- h2o_context(sc)
  spark_df <- invoke(hc, "asDataFrame", h2o.getId(frame), TRUE)
  # Register returned spark_jobj as a table for dplyr
  sdf_register(spark_df, name = name)
}
