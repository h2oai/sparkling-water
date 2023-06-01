package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.{H2OContext, H2OFrame}
import org.apache.spark.sql.DataFrame
import ai.h2o.sparkling.utils.DataFrameSerializationWrappers._

trait HasCalibrationDataFrame extends H2OAlgoParamsBase with HasDataFrameSerializer {

  private val calibrationDataFrame = new NullableDataFrameParam(
    this,
    "calibrationDataFrame",
    "Calibration frame for Platt Scaling. " +
      "To enable usage of the data frame, set the parameter calibrateModel to True.")

  setDefault(calibrationDataFrame -> null)

  def getCalibrationDataFrame(): DataFrame = $(calibrationDataFrame)

  def setCalibrationDataFrame(value: DataFrame): this.type = set(calibrationDataFrame, toWrapper(value))

  private[sparkling] def getCalibrationDataFrameParam(trainingFrame: H2OFrame): Map[String, Any] = {
    Map("calibration_frame" -> convertDataFrameToH2OFrameKey(getCalibrationDataFrame()))
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("calibrationDataFrame" -> "calibration_frame")
  }
}
