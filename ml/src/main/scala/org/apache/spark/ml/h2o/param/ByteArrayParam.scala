package org.apache.spark.ml.h2o.param

import org.apache.spark.ml.param.{Param, Params}
import org.json4s.JValue
import org.json4s.JsonAST.{JArray, JInt}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

class ByteArrayWrapper(val arr: Array[Byte]) extends Serializable

class ByteArrayParam(parent: Params, name: String, doc: String, isValid: ByteArrayWrapper => Boolean)
  extends Param[ByteArrayWrapper](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  override def jsonEncode(value: ByteArrayWrapper): String = {
    val encoded: JValue = JArray(value.arr.toSeq.map(JInt(_)).toList)
    compact(render(encoded))
  }

  override def jsonDecode(json: String): ByteArrayWrapper = {
    parse(json) match {
      case JArray(values) =>
        new ByteArrayWrapper(values.map {
          case JInt(x) =>
            x.byteValue()
          case _ =>
            throw new IllegalArgumentException(s"Cannot decode $json to Byte.")
        }.toArray)
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json as ByteArrayWrapper.")
    }
  }
}