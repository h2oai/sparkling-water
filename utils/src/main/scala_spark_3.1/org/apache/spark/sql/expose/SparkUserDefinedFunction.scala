package org.apache.spark.sql.expose

import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder
import org.apache.spark.sql.expressions
import org.apache.spark.sql.types.DataType

object SparkUserDefinedFunction {
  def apply(
      f: AnyRef,
      dataType: DataType,
      inputEncoders: Seq[Option[ExpressionEncoder[_]]] = Nil,
      outputEncoder: Option[ExpressionEncoder[_]] = None,
      name: Option[String] = None,
      nullable: Boolean = true,
      deterministic: Boolean = true): expressions.SparkUserDefinedFunction = {
    expressions.SparkUserDefinedFunction(f, dataType, inputEncoders, outputEncoder, name, nullable, deterministic)
  }
}
