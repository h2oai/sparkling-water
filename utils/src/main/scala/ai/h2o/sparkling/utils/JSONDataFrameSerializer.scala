package ai.h2o.sparkling.utils

import org.apache.spark.sql.{DataFrame, DataTypeExtensions, Encoders}
import org.apache.spark.sql.DataTypeExtensions._
import org.apache.spark.sql.types.StructType
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

class JSONDataFrameSerializer extends DataFrameSerializer {
  def serialize(df: DataFrame): JValue = {
    val schemaJsonValue = df.schema.jsonValue
    val rows = df.toJSON.collect().map(parse(_)).toList
    val rowsJsonArray = JArray(rows)
    JObject(JField("schema", schemaJsonValue), JField("rows", rowsJsonArray))
  }

  def deserialize(input: JValue): DataFrame = {
    val objMap = input.asInstanceOf[JObject].obj.toMap
    val schema = DataTypeExtensions.jsonToDateType(objMap("schema")).asInstanceOf[StructType]
    val rows = objMap("rows").asInstanceOf[JArray].arr.map(v => compact(render(v)))
    val spark = SparkSessionUtils.active
    val jsonDataset = spark.createDataset(rows)(Encoders.STRING)
    SparkSessionUtils.active.read.schema(schema).json(jsonDataset)
  }
}
