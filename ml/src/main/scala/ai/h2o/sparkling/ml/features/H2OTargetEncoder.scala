package ai.h2o.sparkling.ml.features

import ai.h2o.sparkling.H2OContext
import ai.h2o.sparkling.backend.utils.RestCommunication
import ai.h2o.sparkling.ml.internals.H2OModel
import ai.h2o.sparkling.ml.models.{H2OTargetEncoderBase, H2OTargetEncoderModel}
import ai.h2o.sparkling.ml.params.{EnumParamValidator, H2OTargetEncoderProblemType}
import ai.h2o.sparkling.ml.utils.EstimatorCommonUtils
import ai.h2o.targetencoding._
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{BooleanType, DoubleType, StringType}

class H2OTargetEncoder(override val uid: String)
  extends Estimator[H2OTargetEncoderModel]
  with H2OTargetEncoderBase
  with DefaultParamsWritable
  with RestCommunication
  with EstimatorCommonUtils {

  def this() = this(Identifiable.randomUID("H2OTargetEncoder"))

  override def fit(dataset: Dataset[_]): H2OTargetEncoderModel = {
    val h2oModel = if (getInputCols().isEmpty) {
      None
    } else {
      val h2oContext = H2OContext.ensure(
        "H2OContext needs to be created in order to use target encoding. Please create one as H2OContext.getOrCreate().")
      val problemType = H2OTargetEncoderProblemType.valueOf(getProblemType())
      val toConvertDF = if (problemType == H2OTargetEncoderProblemType.Regression) {
        dataset.withColumn(getLabelCol(), col(getLabelCol()).cast(DoubleType))
      } else {
        dataset.toDF()
      }
      val input = h2oContext.asH2OFrame(toConvertDF)
      val distinctInputCols = getInputCols().flatten.distinct
      val toCategorical = if (isLabelColCategorical(problemType, dataset)) {
        distinctInputCols ++ Seq(getLabelCol())
      } else {
        distinctInputCols
      }
      val converted = input.convertColumnsToCategorical(toCategorical)
      val params = Map(
        "data_leakage_handling" -> getHoldoutStrategy(),
        "blending" -> getBlendedAvgEnabled(),
        "inflection_point" -> getBlendedAvgInflectionPoint(),
        "smoothing" -> getBlendedAvgSmoothing(),
        "response_column" -> getLabelCol(),
        "fold_column" -> getFoldCol(),
        "columns_to_encode" -> getInputCols(),
        "seed" -> getNoiseSeed(),
        "training_frame" -> converted.frameId)
      val targetEncoderModelId = trainAndGetDestinationKey(s"/3/ModelBuilders/targetencoder", params)
      input.delete()
      Some(H2OModel(targetEncoderModelId))
    }
    val model = new H2OTargetEncoderModel(uid, h2oModel).setParent(this)
    copyValues(model)
  }

  private def isLabelColCategorical(problemType: H2OTargetEncoderProblemType, dataset: Dataset[_]): Boolean = {
    problemType == H2OTargetEncoderProblemType.Classification ||
    (problemType == H2OTargetEncoderProblemType.Auto && {
      val dataType = dataset.select(col((getLabelCol()))).schema.fields.head.dataType
      dataType.isInstanceOf[StringType] || dataType.isInstanceOf[BooleanType]
    })
  }

  override def copy(extra: ParamMap): H2OTargetEncoder = defaultCopy(extra)

  //
  // Parameter Setters
  //
  def setFoldCol(value: String): this.type = set(foldCol, value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setInputCols(values: Array[String]): this.type = set(inputCols, values.map(v => Array(v)))

  def setInputCols(groups: Array[Array[String]]): this.type = set(inputCols, groups)

  def setOutputCols(values: Array[String]): this.type = set(outputCols, values)

  def setHoldoutStrategy(value: String): this.type = {
    set(
      holdoutStrategy,
      EnumParamValidator.getValidatedEnumValue[TargetEncoderModel.DataLeakageHandlingStrategy](value))
  }

  def setBlendedAvgEnabled(value: Boolean): this.type = set(blendedAvgEnabled, value)

  def setBlendedAvgInflectionPoint(value: Double): this.type = set(blendedAvgInflectionPoint, value)

  def setBlendedAvgSmoothing(value: Double): this.type = {
    require(value > 0.0, "The smoothing value has to be a positive number.")
    set(blendedAvgSmoothing, value)
  }

  def setNoise(value: Double): this.type = {
    require(value >= 0.0, "Noise can't be a negative value.")
    set(noise, value)
  }

  def setNoiseSeed(value: Long): this.type = set(noiseSeed, value)

  def setProblemType(value: String): this.type = {
    val validated = EnumParamValidator.getValidatedEnumValue[H2OTargetEncoderProblemType](value)
    set(problemType, validated)
  }
}

object H2OTargetEncoder extends DefaultParamsReadable[H2OTargetEncoder]
