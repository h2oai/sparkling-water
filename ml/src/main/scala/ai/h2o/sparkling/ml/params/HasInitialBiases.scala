package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.H2OFrame
import org.apache.spark.ml.linalg.DenseVector

trait HasInitialBiases extends H2OAlgoParamsBase {
  private val initialBiases = new NullableVectorArrayParam(
    this,
    "initialBiases",
    "A array of weight vectors to be used for bias initialization of every network layer." +
      "If this parameter is set, the parameter 'initialWeights' has to be set as well.")

  setDefault(initialBiases -> null)

  def getInitialBiases(): Array[DenseVector] = $(initialBiases)

  def setInitialBiases(value: Array[DenseVector]): this.type = set(initialBiases, value)

  private[sparkling] def getInitialBiasesParam(trainingFrame: H2OFrame): Map[String, Any] = {
    Map("initial_biases" -> convertVectorArrayToH2OFrameKeyArray(getInitialBiases()))
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("initialBiases" -> "initial_biases")
  }
}
