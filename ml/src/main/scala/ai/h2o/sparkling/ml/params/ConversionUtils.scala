package ai.h2o.sparkling.ml.params

import java.util
import scala.collection.JavaConverters._

object ConversionUtils {
  def toDoubleArray(input: util.ArrayList[Double]): Array[Double] = input.asScala.toArray
}
