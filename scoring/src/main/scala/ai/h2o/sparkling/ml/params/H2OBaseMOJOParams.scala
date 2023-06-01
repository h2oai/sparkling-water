package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.ml.models.H2OMOJOSettings
import org.apache.spark.ml.param._

trait H2OBaseMOJOParams extends Params with HasDataFrameSerializer {

  protected final val convertUnknownCategoricalLevelsToNa = new BooleanParam(
    this,
    "convertUnknownCategoricalLevelsToNa",
    "If set to 'true', the model converts unknown categorical levels to NA during making predictions.")

  protected final val convertInvalidNumbersToNa = new BooleanParam(
    this,
    "convertInvalidNumbersToNa",
    "If set to 'true', the model converts invalid numbers to NA during making predictions.")

  setDefault(
    convertUnknownCategoricalLevelsToNa -> H2OMOJOSettings.default.convertUnknownCategoricalLevelsToNa,
    convertInvalidNumbersToNa -> H2OMOJOSettings.default.convertInvalidNumbersToNa,
    dataFrameSerializer -> H2OMOJOSettings.default.dataFrameSerializer)

  def getConvertUnknownCategoricalLevelsToNa(): Boolean = $(convertUnknownCategoricalLevelsToNa)

  def getConvertInvalidNumbersToNa(): Boolean = $(convertInvalidNumbersToNa)

  def setDataFrameSerializer(fullClassName: String): this.type = set(dataFrameSerializer, fullClassName)
}
