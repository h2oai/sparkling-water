package org.apache.spark.ml.util.expose

import org.apache.spark.SparkContext
import org.json4s.JValue

object DefaultParamsReader {
  def loadMetadata(path: String, sc: SparkContext): Metadata = {
    val m = org.apache.spark.ml.util.DefaultParamsReader.loadMetadata(path, sc)
    Metadata(m.className, m.uid, m.params, m.metadataJson)
  }

  case class Metadata(className: String, uid: String, params: JValue, metadataJson: String)

}
