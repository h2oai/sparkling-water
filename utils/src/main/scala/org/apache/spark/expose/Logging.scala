package org.apache.spark.expose

/**
  * The trait is used to make visible the logic of org.apache.spark.internal.Logging.
  */
trait Logging extends org.apache.spark.internal.Logging with Serializable
