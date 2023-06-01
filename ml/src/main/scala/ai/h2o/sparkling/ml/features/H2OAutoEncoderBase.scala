package ai.h2o.sparkling.ml.features

import ai.h2o.sparkling.H2OFrame
import ai.h2o.sparkling.ml.models.H2OAutoEncoderMOJOModel
import ai.h2o.sparkling.ml.params.{H2OAutoEncoderExtraParams, HasInputCols}
import hex.Model
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

abstract class H2OAutoEncoderBase[P <: Model.Parameters: ClassTag]
  extends H2OFeatureEstimator[P]
  with H2OAutoEncoderExtraParams
  with HasInputCols {

  override private[sparkling] def getH2OAlgorithmParams(trainingFrame: H2OFrame): Map[String, Any] = {
    super.getH2OAlgorithmParams(trainingFrame) ++ Map("autoencoder" -> true)
  }

  override def fit(dataset: Dataset[_]): H2OAutoEncoderMOJOModel = {
    val model = super.fit(dataset).asInstanceOf[H2OAutoEncoderMOJOModel]
    copyExtraParams(model)
    model
  }

  private[sparkling] def getWeightCol(): String

  override private[sparkling] def getExcludedCols(): Seq[String] = {
    super.getExcludedCols() ++ Seq(getWeightCol())
      .flatMap(Option(_)) // Remove nulls
  }

  override protected def createMOJOUID(): String = Identifiable.randomUID("AutoEncoder")
}
