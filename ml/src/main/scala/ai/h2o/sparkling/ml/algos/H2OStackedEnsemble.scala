package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.ml.internals.H2OModel
import ai.h2o.sparkling.ml.models.{H2OBinaryModel, H2OMOJOModel, H2OStackedEnsembleMOJOModel}
import ai.h2o.sparkling.ml.params.H2OStackedEnsembleParams
import ai.h2o.sparkling.H2OContext
import ai.h2o.sparkling.ml.utils.H2OParamsReadable
import hex.Model
import hex.ensemble.StackedEnsembleModel.StackedEnsembleParameters
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

/**
  * H2O Stacked Ensemble algorithm exposed via Spark ML pipelines.
  *
  */
class H2OStackedEnsemble(override val uid: String)
  extends H2OSupervisedAlgorithm[StackedEnsembleParameters]
  with H2OStackedEnsembleParams {

  def this() = this(Identifiable.randomUID(classOf[H2OStackedEnsemble].getSimpleName))

  override def fit(dataset: Dataset[_]): H2OStackedEnsembleMOJOModel = {

    if (getBaseAlgorithms().length < 2) {
      throw new IllegalArgumentException("Stacked Ensemble needs at least two base algorithms to operate.")
    }

    val (train, valid) = prepareDatasetForFitting(dataset)
    prepareH2OTrainFrameForFitting(train)

    val baseModels = getBaseAlgorithms().map(alg => alg.trainH2OModel(train, valid))

    val params = getH2OAlgorithmParams(train) ++
      Map(
        "training_frame" -> train.frameId,
        "model_id" -> convertModelIdToKey(getModelId()),
        "base_models" -> modelsIdsToString(baseModels)) ++
      valid
        .map { fr =>
          Map("validation_frame" -> fr.frameId)
        }
        .getOrElse(Map())

    val modelId = trainAndGetDestinationKey(s"/99/ModelBuilders/stackedensemble", params)
    val model = H2OModel(modelId)
    val withCrossValidationModels = if (hasParam("keepCrossValidationModels")) {
      getOrDefault(getParam("keepCrossValidationModels")).asInstanceOf[Boolean]
    } else {
      false
    }

    val result = model
      .toMOJOModel(createMOJOUID(), createMOJOSettings(), withCrossValidationModels)
      .asInstanceOf[H2OStackedEnsembleMOJOModel]

    if (H2OContext.get().forall(_.getConf.isModelPrintAfterTrainingEnabled)) {
      println(result)
    }

    deleteRegisteredH2OFrames()

    if (getKeepBinaryModels()) {
      val downloadedModel = downloadBinaryModel(modelId, H2OContext.ensure().getConf)
      binaryModel = Some(H2OBinaryModel.read("file://" + downloadedModel.getAbsolutePath, Some(modelId)))
    } else {
      model.tryDelete()
      deleteModels(baseModels)
    }

    result
  }

  private def modelsIdsToString(baseModels: Array[H2OMOJOModel]) = {
    modelsIds(baseModels).mkString("[", ",", "]")
  }

  private def modelsIds(baseModels: Seq[H2OMOJOModel]): Seq[String] = {
    baseModels.map(_.mojoFileName)
  }

  private def deleteModels(baseModels: Seq[H2OMOJOModel]): Unit = {
    modelsIds(baseModels).foreach(H2OModel(_).tryDelete())
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = schema

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  override protected def paramTag: ClassTag[StackedEnsembleParameters] =
    scala.reflect.classTag[StackedEnsembleParameters]

  override private[sparkling] def getInputCols(): Array[String] = getFeaturesCols()

  override private[sparkling] def setInputCols(value: Array[String]): this.type = setFeaturesCols(value)
}

object H2OStackedEnsemble extends H2OParamsReadable[H2OStackedEnsemble]
