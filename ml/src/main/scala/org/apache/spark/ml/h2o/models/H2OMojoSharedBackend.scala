package org.apache.spark.ml.h2o.models

import org.apache.spark.ml.h2o.param.H2OModelParams
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Model => SparkModel}
import org.apache.spark.sql.{DataFrame, Dataset}

class H2OMojoSharedBackend(val mojoData: Array[Byte], override val uid: String)
  extends SparkModel[H2OMOJOModel] with H2OModelParams with MLWritable {

  def this(mojoData: Array[Byte]) = this(mojoData, Identifiable.randomUID("mojoModel"))

  abstract def transform(dataset: Dataset[_]): DataFrame
}
