package org.apache.spark.ml.h2o

import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{SchemaRDD, StructType}

/**
 * ML Transformation:
 *  H2O DataFrame into
 */
class RDD2DataFrame extends Transformer {
  override def transform(dataset: SchemaRDD, paramMap: ParamMap): SchemaRDD = ???

  override private[ml] def transformSchema(schema: StructType, paramMap: ParamMap): StructType = ???
}
