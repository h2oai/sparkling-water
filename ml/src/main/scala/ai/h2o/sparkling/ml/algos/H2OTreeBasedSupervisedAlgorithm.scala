package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.ml.models.H2OTreeBasedSupervisedMOJOModel
import hex.Model
import org.apache.spark.sql.Dataset

import scala.reflect.ClassTag

abstract class H2OTreeBasedSupervisedAlgorithm[P <: Model.Parameters: ClassTag]
  extends H2OSupervisedAlgorithmWithFoldColumn[P] {

  override def fit(dataset: Dataset[_]): H2OTreeBasedSupervisedMOJOModel = {
    super.fit(dataset).asInstanceOf[H2OTreeBasedSupervisedMOJOModel]
  }

  def getNtrees(): Int

  def setNtrees(value: Int): this.type
}
