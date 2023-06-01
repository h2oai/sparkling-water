package ai.h2o.sparkling.ml.params

import org.apache.spark.ml.param.{Param, Params}
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

class MapStringStringParam(parent: Params, name: String, doc: String, isValid: Map[String, String] => Boolean)
  extends Param[Map[String, String]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  override def jsonEncode(value: Map[String, String]): String = {
    val encoded = value.map(p => p._1 -> JString(p._2)).toList
    compact(render(encoded))
  }

  override def jsonDecode(json: String): Map[String, String] = {
    implicit val format = DefaultFormats

    parse(json) match {
      case JObject(pairs) =>
        pairs.map {
          case (name, value) =>
            name -> value.extract[String]
        }.toMap
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Map[H2OMetric, Double].")
    }
  }
}
