package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.{H2OContext, H2OFrame}
import ai.h2o.sparkling.backend.utils.RestCommunication
import ai.h2o.sparkling.ml.internals.H2OModel
import ai.h2o.sparkling.ml.models.{H2OBinaryModel, H2OMOJOModel, H2OMOJOSettings}
import ai.h2o.sparkling.ml.params.{H2OAlgoParamsBase, H2OCommonParams}
import hex.Model
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

abstract class H2OEstimator[P <: Model.Parameters: ClassTag]
  extends Estimator[H2OMOJOModel]
  with H2OAlgoParamsBase
  with H2OCommonParams
  with H2OAlgoCommonUtils
  with H2OTrainFramePreparation
  with DefaultParamsWritable
  with RestCommunication {

  protected def getModelId(): String

  // Class tag for parameters to get runtime class
  protected def paramTag: ClassTag[P]

  protected var parameters: P = paramTag.runtimeClass.newInstance().asInstanceOf[P]

  override def fit(dataset: Dataset[_]): H2OMOJOModel = {
    val (train, valid) = prepareDatasetForFitting(dataset)
    prepareH2OTrainFrameForFitting(train)

    val mojoModel = trainH2OModel(train, valid)

    deleteRegisteredH2OFrames()
    mojoModel
  }

  private[sparkling] def trainH2OModel(train: H2OFrame, valid: Option[H2OFrame]): H2OMOJOModel = {

    val params = getH2OAlgorithmParams(train) ++
      Map("training_frame" -> train.frameId, "model_id" -> convertModelIdToKey(getModelId())) ++
      valid
        .map { fr =>
          Map("validation_frame" -> fr.frameId)
        }
        .getOrElse(Map())
    val modelId = trainAndGetDestinationKey(s"/3/ModelBuilders/${parameters.algoName().toLowerCase}", params)
    val model = H2OModel(modelId)
    val withCrossValidationModels = if (hasParam("keepCrossValidationModels")) {
      getOrDefault(getParam("keepCrossValidationModels")).asInstanceOf[Boolean]
    } else {
      false
    }

    val result = model.toMOJOModel(createMOJOUID(), createMOJOSettings(), withCrossValidationModels)
    if (H2OContext.get().forall(_.getConf.isModelPrintAfterTrainingEnabled)) {
      println(result)
    }
    if (getKeepBinaryModels()) {
      val downloadedModel = downloadBinaryModel(modelId, H2OContext.ensure().getConf)
      binaryModel = Some(H2OBinaryModel.read("file://" + downloadedModel.getAbsolutePath, Some(modelId)))
    } else {
      model.tryDelete()
    }
    result
  }

  protected def createMOJOUID(): String = Identifiable.randomUID(parameters.algoName())

  protected def createMOJOSettings(): H2OMOJOSettings = {
    H2OMOJOSettings(
      convertUnknownCategoricalLevelsToNa = this.getConvertUnknownCategoricalLevelsToNa(),
      convertInvalidNumbersToNa = this.getConvertInvalidNumbersToNa(),
      dataFrameSerializer = this.getDataFrameSerializer())
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}
