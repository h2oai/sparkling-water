package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.ml.utils.Utils
import hex.genmodel.easy.EasyPredictModelWrapper
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

trait PredictionWithStageProbabilities {
  protected def getStageProbabilitiesSchema(model: EasyPredictModelWrapper): DataType = {
    val valueType = ArrayType(DoubleType, containsNull = false)
    val stageProbabilityFields = model.getResponseDomainValues.map(StructField(_, valueType, nullable = false))
    StructType(stageProbabilityFields)
  }
}
