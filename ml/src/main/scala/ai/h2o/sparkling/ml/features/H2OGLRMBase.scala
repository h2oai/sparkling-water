package ai.h2o.sparkling.ml.features

import ai.h2o.sparkling.ml.models.H2OGLRMMOJOModel
import ai.h2o.sparkling.ml.params.{H2OGLRMExtraParams, HasInputCols}
import hex.Model
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

abstract class H2OGLRMBase[P <: Model.Parameters: ClassTag]
  extends H2ODimReductionEstimator[P]
  with H2OGLRMExtraParams
  with HasInputCols {

  override def fit(dataset: Dataset[_]): H2OGLRMMOJOModel = {
    val model = super.fit(dataset).asInstanceOf[H2OGLRMMOJOModel]
    copyExtraParams(model)
    model
  }
}
