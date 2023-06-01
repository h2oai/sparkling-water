package ai.h2o.sparkling.ml.params

import org.apache.spark.ml.param.{Param, Params}
import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

class NullableMapStringDoubleParam(parent: Params, name: String, doc: String, isValid: Map[String, Double] => Boolean)
  extends Param[Map[String, Double]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  override def jsonEncode(value: Map[String, Double]): String = {
    val encoded = if (value == null) {
      JNull
    } else {
      JObject(value.map(p => p._1 -> DoubleParam.jValueEncode(p._2)).toList)
    }
    compact(render(encoded))
  }

  override def jsonDecode(json: String): Map[String, Double] = {
    parse(json) match {
      case JNull => null
      case JObject(pairs) =>
        pairs.map {
          case (name, value) =>
            (name, DoubleParam.jValueDecode(value))
        }.toMap
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Map[String, Double].")
    }
  }
}
