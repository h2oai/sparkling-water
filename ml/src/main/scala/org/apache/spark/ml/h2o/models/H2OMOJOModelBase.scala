package org.apache.spark.ml.h2o.models

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.h2o.param.H2OMOJOModelParams
import org.apache.spark.ml.util.{MLWritable, MLWriter}
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.ml.{Model => SparkModel}

abstract class H2OMOJOModelBase[T <: SparkModel[T]]
  extends SparkModel[T] with H2OMOJOModelParams with MLWritable with HasMojoData {

  def getPredictionSchema(): Seq[StructField]

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    // Here we should check validity of input schema however
    // in theory user can pass invalid schema with missing columns
    // and model will be able to still provide a prediction
    StructType(schema.fields ++ getPredictionSchema())
  }

  override def write: MLWriter = new H2OMOJOWriter(this, getMojoData)
}
