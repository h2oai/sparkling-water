package ai.h2o.sparkling.ml.models

import hex.genmodel.easy.EasyPredictModelWrapper
import org.apache.spark.sql.types._

trait PredictionWithContributions {
  protected def getContributionsSchema(model: EasyPredictModelWrapper): DataType = {
    val individualContributions = model.getContributionNames().map(StructField(_, FloatType, nullable = false))
    StructType(individualContributions)
  }
}
