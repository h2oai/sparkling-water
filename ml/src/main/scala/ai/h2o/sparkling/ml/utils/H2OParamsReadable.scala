package ai.h2o.sparkling.ml.utils

import org.apache.spark.ml.util.{MLReadable, MLReader}

class H2OParamsReadable[T] extends MLReadable[T] {

  override def read: MLReader[T] = new H2OReaderBase[T]
}
