package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.ml.models.H2OUnsupervisedMOJOModel
import hex.Model
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

abstract class H2OUnsupervisedAlgorithm[P <: Model.Parameters: ClassTag] extends H2OAlgorithm[P] {

  override def fit(dataset: Dataset[_]): H2OUnsupervisedMOJOModel = {
    super.fit(dataset).asInstanceOf[H2OUnsupervisedMOJOModel]
  }
}
