package ai.h2o.sparkling.ml.features

import ai.h2o.sparkling.ml.algos.H2OEstimator
import ai.h2o.sparkling.ml.models.{H2OFeatureEstimatorBase, H2OFeatureMOJOModel}
import hex.Model
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

abstract class H2OFeatureEstimator[P <: Model.Parameters: ClassTag]
  extends H2OEstimator[P]
  with H2OFeatureEstimatorBase {

  override def fit(dataset: Dataset[_]): H2OFeatureMOJOModel = {
    super.fit(dataset).asInstanceOf[H2OFeatureMOJOModel]
  }
}
