package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.ml.models.H2OFeatureEstimatorBase
import org.apache.spark.sql.types.{StructField, StructType}

trait H2ODimReductionExtraParams extends H2OFeatureEstimatorBase with HasOutputCol with HasInputColsOnMOJO {

  protected override def outputSchema: Seq[StructField] = {
    val outputType = org.apache.spark.ml.linalg.SQLDataTypes.VectorType
    val outputField = StructField(getOutputCol(), outputType, nullable = false)
    Seq(outputField)
  }

  protected override def validate(schema: StructType): Unit = {
    require(getInputCols() != null && getInputCols().nonEmpty, "The list of input columns can't be null or empty!")
    require(getOutputCol() != null, "The output column can't be null!")
    val fieldNames = schema.fieldNames
    getInputCols().foreach { inputCol =>
      require(
        fieldNames.contains(inputCol),
        s"The specified input column '$inputCol' was not found in the input dataset!")
    }
    require(
      !fieldNames.contains(getOutputCol()),
      s"The output column '${getOutputCol()}' is already present in the dataset!")
  }

  protected def copyExtraParams(to: H2ODimReductionExtraParams): Unit = {
    to.set(to.inputCols -> getInputCols())
    to.setOutputCol(getOutputCol())
  }
}
