package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.H2OFrame
import org.apache.spark.expose.Logging

trait HasInteractionPairs extends H2OAlgoParamsBase with Logging {
  private val interactionPairs =
    new NullableStringPairArrayParam(this, "interactionPairs", "A list of pairwise (first order) column interactions.")

  setDefault(interactionPairs -> null)

  private[sparkling] def getInteractionPairsParam(trainingFrame: H2OFrame): Map[String, Any] = Map.empty

  def getInteractionPairs(): Array[(String, String)] = null

  def setInteractionPairs(value: Array[(String, String)]): this.type = {
    logWarning("Interaction pairs are not supported.")
    this
  }
}
