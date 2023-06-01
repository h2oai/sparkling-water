package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.H2OFrame
import org.apache.spark.ml.linalg.DenseMatrix

trait HasInitialWeights extends H2OAlgoParamsBase {
  private val initialWeights = new NullableMatrixArrayParam(
    this,
    "initialWeights",
    "A array of weight matrices to be used for initialization of the neural network. " +
      "If this parameter is set, the parameter 'initialBiases' has to be set as well.")

  setDefault(initialWeights -> null)

  def getInitialWeights(): Array[DenseMatrix] = $(initialWeights)

  def setInitialWeights(value: Array[DenseMatrix]): this.type = set(initialWeights, value)

  private[sparkling] def getInitialWeightsParam(trainingFrame: H2OFrame): Map[String, Any] = {
    Map("initial_weights" -> convertMatrixToH2OFrameKeyArray(getInitialWeights()))
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("initialWeights" -> "initial_weights")
  }
}
