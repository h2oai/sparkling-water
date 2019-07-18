package ai.h2o.sparkling.ml.features

import ai.h2o.sparkling.ml.params.H2OTargetEncoderParams
import org.apache.spark.h2o.Frame
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}

trait H2OTargetEncoderBase extends PipelineStage with H2OTargetEncoderParams {
  override def transformSchema(schema: StructType): StructType = {
    validateSchema(schema)
    StructType(schema.fields ++ getOutputCols().map(StructField(_, DoubleType, nullable = true)))
  }

  private def validateSchema(flatSchema: StructType): Unit = {
    require(getLabelCol() != null, "Label column can't be null!")
    require(getInputCols() != null && getInputCols().nonEmpty, "The list of input columns can't be null or empty!")
    val fields = flatSchema.fields
    val fieldNames = fields.map(_.name)
    require(fieldNames.contains(getLabelCol()),
      s"The specified label column '${getLabelCol()}' was not found in the input dataset!")
    for(inputCol <- getInputCols()) {
      require(fieldNames.contains(inputCol),
        s"The specified input column '$inputCol' was not found in the input dataset!")
    }
    val ioIntersection = getInputCols().intersect(getOutputCols())
    require(ioIntersection.isEmpty,
      s"""The columns [${ioIntersection.map(i => s"'$i'").mkString(", ")}] are specified
         |as input columns and also as output columns. There can't be an overlap.""".stripMargin)
  }

  protected def convertRelevantColumnsToCategorical(frame: Frame): Unit = {
    val relevantColumns = getInputCols() ++ Array(getLabelCol())
    relevantColumns.foreach(frame.toCategoricalCol(_))
  }
}
