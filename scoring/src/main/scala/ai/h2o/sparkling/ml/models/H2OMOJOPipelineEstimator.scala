package ai.h2o.sparkling.ml.models

import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.util.Identifiable

class H2OMOJOPipelineEstimator(override val uid: String) extends Pipeline(uid) {

  def this() = this(Identifiable.randomUID("h2o_mojo_pipeline"))

}
