package org.apache.spark.ml.util.expose

import org.apache.spark.SparkContext
import org.apache.spark.ml.param.Params

object DefaultParamsWriter {
  def saveMetadata(instance: Params, path: String, sc: SparkContext): Unit = {
    org.apache.spark.ml.util.DefaultParamsWriter.saveMetadata(instance, path, sc)
  }
}
