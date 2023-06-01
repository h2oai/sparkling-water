package ai.h2o.sparkling.sql

import org.apache.spark.sql.expose.SparkUserDefinedFunction
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.DataType

/**
  * This objects contains functions from org.apache.spark.sql.functions which are not compatible across all Spark
  * versions supported by Sparkling Water.
  */
object functions {
  def udf(f: AnyRef, dataType: DataType): UserDefinedFunction = {
    SparkUserDefinedFunction(f, dataType, inputEncoders = Nil, outputEncoder = None)
  }
}
