package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.H2OFrame
import org.apache.spark.sql.DataFrame
import ai.h2o.sparkling.utils.DataFrameSerializationWrappers._

trait HasPreTrained extends H2OAlgoParamsBase with HasDataFrameSerializer {
  private val preTrained = new NullableDataFrameParam(
    this,
    "preTrained",
    "A data frame that contains a pre-trained (external) word2vec model.")

  setDefault(preTrained -> null)

  def getPreTrained(): DataFrame = $(preTrained)

  def setPreTrained(value: DataFrame): this.type = set(preTrained, toWrapper(value))

  private[sparkling] def getPreTrainedParam(trainingFrame: H2OFrame): Map[String, Any] = {
    Map("pre_trained" -> convertDataFrameToH2OFrameKey(getPreTrained()))
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("preTrained" -> "pre_trained")
  }
}
