package org.apache.spark.ml.h2o

import hex.deeplearning.{DeepLearningModel, DeepLearning}
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.{Estimator, Model}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.param.{Param,Params}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * Deep learning ML component.
 */
class H2ODeepLearningModel(override val uid: String,
                          model: DeepLearningModel)
  extends Model[H2ODeepLearningModel] {
  
  override def transform(dataset: DataFrame): DataFrame = ???

  override def transformSchema(schema: StructType): StructType = ???
}

class H2ODeepLearning(override val uid: String = "")
                     (implicit hc: H2OContext)
  extends Estimator[H2ODeepLearningModel] with HasDeepLearningParams {

  setDefault(deepLearningParams -> new DeepLearningParameters)

  override def fit(dataset: DataFrame): H2ODeepLearningModel = {
    // Verify parameters - useless here
    transformSchema(dataset.schema, logging = true)
    import hc._

    val params = getDeepLearningParams
    params._train = dataset
    val model = new DeepLearning(params).trainModel().get()
    params._train.remove()
    val dlm = new H2ODeepLearningModel(uid, model)
    dlm
  }

  override def transformSchema(schema: StructType): StructType = ???
}

trait HasDeepLearningParams extends Params {
  val deepLearningParams: Param[DeepLearningParameters] = new Param(this,
    "deepLearningParams", "H2O's DeepLearning parameters")
  def getDeepLearningParams: DeepLearningParameters = getOrDefault(deepLearningParams)

  protected def validateAndTransformSchema(
                                          schema: StructType,
                                          paramMap: ParamMap
                                            ): StructType = {
    ???
  }
}

