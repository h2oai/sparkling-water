package ai.h2o.sparkling.ml.models

import org.apache.spark.ml.param.Params
import org.apache.spark.ml.util.{MLWritable, MLWriter}

trait H2OMOJOWritable extends MLWritable with Params with HasMojoData {
  override def write: MLWriter = new H2OMOJOWriter(this, getMojoData())
}
