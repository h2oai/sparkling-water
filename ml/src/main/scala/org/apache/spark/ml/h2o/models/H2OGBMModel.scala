package org.apache.spark.ml.h2o.models

import hex.genmodel.MojoModel
import org.apache.spark.annotation.Since
import org.apache.spark.ml.util.{Identifiable, MLReadable, MLReader, MLWritable}
import org.apache.spark.sql.SQLContext

class H2OGBMModel(model: MojoModel, mojoData: Array[Byte], override val uid: String)(sqlContext: SQLContext)
  extends H2OMOJOModel[H2OGBMModel](model, mojoData, sqlContext) with MLWritable {

  def this(model: MojoModel, mojoData: Array[Byte])(sqlContext: SQLContext) = this(model, mojoData, Identifiable.randomUID("gbmModel"))(sqlContext)

  override def defaultFileName: String = H2OGBMModel.defaultFileName
}

object H2OGBMModel extends MLReadable[H2OGBMModel] {
  val defaultFileName = "gbm_model"

  @Since("1.6.0")
  override def read: MLReader[H2OGBMModel] = new H2OMOJOModelReader[H2OGBMModel](defaultFileName) {
    override protected def make(model: MojoModel, mojoData: Array[Byte], uid: String)(sqLContext: SQLContext): H2OGBMModel = {
      new H2OGBMModel(model, mojoData, uid)(sqlContext)
    }
  }

  @Since("1.6.0")
  override def load(path: String): H2OGBMModel = super.load(path)
}

