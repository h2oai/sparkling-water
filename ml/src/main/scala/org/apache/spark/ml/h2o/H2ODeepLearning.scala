package org.apache.spark.ml.h2o

import hex.deeplearning.{DeepLearning, DeepLearningModel, DeepLearningParameters}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.ml.{Estimator, PredictionModel}
import org.apache.spark.mllib
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * Deep learning ML component.
 */
class H2ODeepLearningModel(model: DeepLearningModel)
  extends PredictionModel[mllib.linalg.Vector, H2ODeepLearningModel] {

  override protected def predict(features: mllib.linalg.Vector): Double = ???

  override val uid: String = "dlModel"
}

class H2ODeepLearning()(implicit hc: H2OContext)
  extends Estimator[H2ODeepLearningModel] with HasDeepLearningParams {

  override def fit(dataset: DataFrame): H2ODeepLearningModel = {
    import hc._
    val params = new DeepLearningParameters
    params._train = dataset
    val model = new DeepLearning(params).trainModel().get()
    params._train.remove()
    val dlm = new H2ODeepLearningModel(model)
    dlm
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = ???

  override val uid: String = "dl"
}

trait HasDeepLearningParams extends Params {
  val deepLearningParams: Param[DeepLearningParameters] = new Param[DeepLearningParameters]("DL params",
    "deepLearningParams", "H2O's DeepLearning parameters", (t:DeepLearningParameters) => true)

  def getDeepLearningParams: DeepLearningParameters = get(deepLearningParams).get

}

