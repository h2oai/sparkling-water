package ai.h2o.sparkling.ml.models

import org.apache.spark.expose.Logging
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.{DataFrame, Dataset}

abstract class H2OFeatureMOJOModel
  extends H2OMOJOModel
  with H2OFeatureEstimatorBase
  with SpecificMOJOParameters
  with HasMojo
  with H2OMOJOWritable
  with H2OMOJOFlattenedInput
  with Logging {
  override def copy(extra: ParamMap): H2OFeatureMOJOModel = defaultCopy(extra)

  override protected def outputColumnName: String = getClass.getSimpleName + "_temporary"

  protected def mojoUDF: UserDefinedFunction

  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputDF = applyPredictionUdf(dataset, _ => mojoUDF)
    outputDF
      .select("*", s"$outputColumnName.*")
      .drop(outputColumnName)
  }
}
