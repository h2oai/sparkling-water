package ai.h2o.sparkling.ml.algos

import ai.h2o.sparkling.ml.models.{H2OAlgorithmMOJOModel, H2OMOJOSettings}
import ai.h2o.sparkling.ml.params.H2OAlgorithmCommonParams
import hex.Model
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.types.StructType

import scala.reflect.ClassTag

/**
  * Base class for H2O algorithm wrapper as a Spark transformer.
  */
abstract class H2OAlgorithm[P <: Model.Parameters: ClassTag] extends H2OEstimator[P] with H2OAlgorithmCommonParams {

  override private[sparkling] def getInputCols(): Array[String] = getFeaturesCols()

  override private[sparkling] def setInputCols(value: Array[String]): this.type = setFeaturesCols(value)

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = schema

  override def fit(dataset: Dataset[_]): H2OAlgorithmMOJOModel = {
    super.fit(dataset).asInstanceOf[H2OAlgorithmMOJOModel]
  }

  override protected def createMOJOSettings(): H2OMOJOSettings = H2OMOJOSettings.createFromModelParams(this)
}
