package ai.h2o.sparkling.ml.utils

import org.apache.spark.expose.Logging
import org.apache.spark.ml.util._
import org.apache.spark.ml.util.expose.DefaultParamsReader

private[ml] class H2OReaderBase[T] extends MLReader[T] with H2OParamsReader[T] with Logging {

  override def load(path: String): T = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc)
    load(metadata)
  }
}
