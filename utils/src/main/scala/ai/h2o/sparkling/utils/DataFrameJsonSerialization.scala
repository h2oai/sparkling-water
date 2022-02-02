package ai.h2o.sparkling.utils

import java.io.ByteArrayInputStream
import java.util.Base64

import ai.h2o.sparkling.utils.ScalaUtils.withResource
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

object DataFrameJsonSerialization {

  def encodeDataFrame(dataFrame: DataFrame, serializer: DataFrameSerializer): String = {
    val ast = if (dataFrame == null) {
      JNull
    } else {
      val serializedValue = serializer.serialize(dataFrame)
      JObject(JField("serializer", JString(serializer.getClass.getName)), JField("value", serializedValue))
    }
    compact(render(ast))
  }

  def encodeDataFrames(dataFrames: Array[DataFrame], serializer: DataFrameSerializer): String = {
    val encoded = if (dataFrames == null) {
      JNull
    } else {
      JObject(
        JField("serializer", JString(serializer.getClass.getName)),
        JField("dataframes", JArray(dataFrames.toList.map(df => if (df == null) JNull else serializer.serialize(df)))))
    }
    compact(render(encoded))
  }

  def decodeDataFrame(json: String): DataFrame = {
    parse(json) match {
      case JNull =>
        null
      case JObject(fields) =>
        val fieldsMap = fields.toMap[String, JValue]
        val serializerClassName = fieldsMap("serializer").asInstanceOf[JString].values
        val serializer = Class.forName(serializerClassName).newInstance().asInstanceOf[DataFrameSerializer]
        val serializedValue = fieldsMap("value")
        serializer.deserialize(serializedValue)
      case JString(data) =>
        val bytes = Base64.getDecoder().decode(data)
        withResource(new ByteArrayInputStream(bytes)) { byteStream =>
          withResource(new CompatibilityObjectInputStream(byteStream)) { objectStream =>
            val schema = objectStream.readObject().asInstanceOf[StructType]
            val rows = objectStream.readObject().asInstanceOf[java.util.List[Row]]
            SparkSessionUtils.active.createDataFrame(rows, schema)
          }
        }
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to DataFrame.")
    }
  }

  def decodeDataFrames(json: String): Array[DataFrame] = {
    parse(json) match {
      case JNull =>
        null
      case JObject(fields) =>
        val fieldsMap = fields.toMap[String, JValue]

        val serializerClassName = fieldsMap("serializer").asInstanceOf[JString].values
        val serializer = Class.forName(serializerClassName).newInstance().asInstanceOf[DataFrameSerializer]

        val serializedDataFrames = fieldsMap("dataframes")
        serializedDataFrames.children.map {
          case JNull => null
          case json => serializer.deserialize(json)
        }.toArray
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Array[DataFrame].")
    }
  }
}
