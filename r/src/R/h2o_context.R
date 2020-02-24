#' Get or create the H2OContext.
#'
#' @param sc Object of type \code{spark_connection} or \code{spark_jobj}.
#' @export
h2o_context <- function(sc, conf = NULL, username = NA_character_, password = NA_character_) {
  print("Method h2o_context is deprecated and will be deleted in major release 3.30. Create instance of H2OContext as hc <- H2OContext.getOrCreate().")
  if (is.null(conf)) {
    conf <- H2OConf()
  }
  if (!is.na(username)) {
    print("Providing username via username parameter on H2OContext is deprecated. Please use setUserName H2OConf object.")
    conf$setUserName(username)
  }

  if (!is.na(password)) {
    print("Providing password via password parameter on H2OContext is deprecated. Please use setUserName H2OConf object.")
    conf$setPassword(password)
  }

  H2OContext.getOrCreate(conf)$jhc
}

#' Open the H2O Flow UI in a browser
#'
#' @param sc Object of type \code{spark_connection}.
#' @export
h2o_flow <- function(sc) {
  print("Method h2o_flow is deprecated and will be deleted in major release 3.30. First create instance of H2OContext as hc <- H2OContext.getOrCreate() and then call hc$openFlow().")
  H2OContext.getOrCreate()$openFlow()
}

#' Convert a Spark DataFrame to an H2O Frame
#'
#' @param sc Object of type \code{spark_connection}.
#' @param frame A \code{spark_dataframe}.
#' @param name The name of the H2OFrame.
#' @export
as_h2o_frame <- function(sc, frame, name=NULL) {
  print("Method as_h2o_frame is deprecated and will be deleted in major release 3.30. First create instance of H2OContext as hc <- H2OContext.getOrCreate() and then call hc$asH2OFrame().")
  H2OContext.getOrCreate()$asH2OFrame(frame, name)
}

#' Convert an H2O Frame to a Spark DataFrame
#'
#' @param sc Object of type \code{spark_connection}.
#' @param frame An \code{H2OFrame}.
#' @param name The name to assign the data frame in Spark.
#' @export
as_spark_dataframe <- function(sc, frame, name = paste(deparse(substitute(sc)), collapse="")) {
  print("Method as_spark_dataframe is deprecated and will be deleted in major release 3.30. First create instance of H2OContext as hc <- H2OContext.getOrCreate() and then call hc$asSparkFrame().")
  H2OContext.getOrCreate()$asSparkFrame(frame)
}
