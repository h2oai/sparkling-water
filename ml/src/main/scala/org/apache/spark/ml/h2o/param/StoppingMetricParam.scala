package org.apache.spark.ml.h2o.param

import hex.ScoreKeeper
import org.apache.spark.ml.param.Params

class StoppingMetricParam private[h2o](parent: Params, name: String, doc: String,
                                       isValid: ScoreKeeper.StoppingMetric => Boolean)
  extends EnumParam[ScoreKeeper.StoppingMetric](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)
}
