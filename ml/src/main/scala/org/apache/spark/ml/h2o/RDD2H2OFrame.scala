package org.apache.spark.ml.h2o

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.StructType

/**
 * ML Transformation:
 *  H2O DataFrame into
 */
class RDD2H2OFrame extends Transformer {
  override def transform(dataset: DataFrame): DataFrame = ???

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = ???

  override def copy(extra: ParamMap): RDD2H2OFrame = ???

  override val uid: String = "rdd2h2oFram_"
}
