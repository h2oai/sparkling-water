package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.H2OFrame
import org.apache.spark.expose.Logging
import org.apache.spark.sql.DataFrame

trait HasBlendingDataFrame extends H2OAlgoParamsBase with Logging with HasDataFrameSerializer {

  val uid: String

  private val blendingDataFrame = new NonSerializableNullableDataFrameParam(
    this,
    "blendingDataFrame",
    "This parameter is used for  computing the predictions that serve as the training frame for the meta-learner." +
      " If provided, this triggers blending mode on the stacked ensemble training stage. Blending mode is faster" +
      " than cross-validating the base learners (though these ensembles may not perform as well as the Super Learner" +
      " ensemble).")

  setDefault(blendingDataFrame -> null)

  def getBlendingDataFrame(): DataFrame = $(blendingDataFrame)

  def setBlendingDataFrame(value: DataFrame): this.type = set(blendingDataFrame, value)

  private[sparkling] def getBlendingDataFrameParam(trainingFrame: H2OFrame): Map[String, Any] = {
    Map("blending_frame" -> convertDataFrameToH2OFrameKey(getBlendingDataFrame()))
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("blendingDataFrame" -> "blending_frame")
  }
}
