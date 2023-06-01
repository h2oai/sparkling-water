package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.H2OFrame
import ai.h2o.sparkling.ml.algos.H2OAlgorithm
import hex.Model

trait HasBaseAlgorithms extends H2OAlgoParamsBase {

  private val base_algorithms = new NullableAlgoArrayParam(this, "baseAlgorithms", "An array of base algorithms")

  setDefault(base_algorithms -> null)

  def getBaseAlgorithms(): Array[H2OAlgorithm[_ <: Model.Parameters]] = $(base_algorithms)

  def setBaseAlgorithms(value: Array[H2OAlgorithm[_ <: Model.Parameters]]): this.type = set(base_algorithms, value)

  private[sparkling] def getBaseAlgorithmsParam(trainingFrame: H2OFrame): Map[String, Any] = {
    // the base_algorithms parameter isn't used by H2O backend
    Map.empty
  }
}
