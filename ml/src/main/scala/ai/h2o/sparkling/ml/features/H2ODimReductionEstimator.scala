package ai.h2o.sparkling.ml.features

import ai.h2o.sparkling.ml.models.H2ODimReductionMOJOModel
import ai.h2o.sparkling.ml.params.{H2ODimReductionExtraParams, HasInputCols}
import hex.Model
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

abstract class H2ODimReductionEstimator[P <: Model.Parameters: ClassTag]
  extends H2OFeatureEstimator[P]
  with H2ODimReductionExtraParams
  with HasInputCols {

  override def fit(dataset: Dataset[_]): H2ODimReductionMOJOModel = {
    val model = super.fit(dataset).asInstanceOf[H2ODimReductionMOJOModel]
    copyExtraParams(model)
    model
  }
}
