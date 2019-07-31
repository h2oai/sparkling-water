package ai.h2o.sparkling.ml.models

import org.apache.spark.ml.util.{MLReadable, MLReader}

trait H2OMOJOReadable[T <: HasMojoData] extends MLReadable[T] {
  override def read: MLReader[T] = new H2OMOJOReader[T]
}
