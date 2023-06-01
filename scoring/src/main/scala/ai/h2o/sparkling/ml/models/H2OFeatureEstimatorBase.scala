package ai.h2o.sparkling.ml.models

import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql.types.{StructField, StructType}

trait H2OFeatureEstimatorBase extends PipelineStage {

  protected def outputSchema: Seq[StructField]

  protected def validate(schema: StructType): Unit

  override def transformSchema(schema: StructType): StructType = {
    validate(schema)
    StructType(schema.fields ++ outputSchema)
  }
}
