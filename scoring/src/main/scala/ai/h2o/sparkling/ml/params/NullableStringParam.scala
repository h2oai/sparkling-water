package ai.h2o.sparkling.ml.params

import org.apache.spark.ml.param.{Param, Params}
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{JNull, JString, JValue}

class NullableStringParam(parent: Params, name: String, doc: String, isValid: String => Boolean)
  extends Param[String](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  override def jsonEncode(value: String): String = {
    val encoded: JValue = if (value == null) {
      JNull
    } else {
      JString(value)
    }
    compact(render(encoded))
  }

  override def jsonDecode(json: String): String = {
    parse(json) match {
      case JNull =>
        null
      case JString(s) =>
        s
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to String.")
    }
  }
}
