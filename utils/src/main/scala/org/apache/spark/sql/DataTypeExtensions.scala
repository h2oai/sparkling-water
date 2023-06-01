package org.apache.spark.sql

import org.apache.spark.sql.types.DataType
import org.json4s.JsonAST.JValue

object DataTypeExtensions {

  implicit class DataTypeWrapper(dataType: DataType) {
    def jsonValue: JValue = dataType.jsonValue
  }

  def jsonToDateType(value: JValue): DataType = DataType.parseDataType(value)
}
