package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.ml.models.H2OTreeBasedUnsupervisedMOJOModel
import hex.Model
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

abstract class H2OTreeBasedUnsupervisedAlgorithm[P <: Model.Parameters: ClassTag] extends H2OUnsupervisedAlgorithm[P] {

  override def fit(dataset: Dataset[_]): H2OTreeBasedUnsupervisedMOJOModel = {
    super.fit(dataset).asInstanceOf[H2OTreeBasedUnsupervisedMOJOModel]
  }

  def getNtrees(): Int

  def setNtrees(value: Int): this.type
}
