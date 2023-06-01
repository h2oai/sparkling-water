package ai.h2o.sparkling.ml.params

import org.apache.spark.ml.param.{Param, ParamPair, Params}
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{JNull, JString}

import scala.collection.JavaConverters._

class NullableStringArrayParam(parent: Params, name: String, doc: String, isValid: Array[String] => Boolean)
  extends Param[Array[String]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[java.lang.String]): ParamPair[Array[String]] =
    w(value.asScala.map(_.asInstanceOf[String]).toArray)

  override def jsonEncode(value: Array[String]): String = {
    if (value == null) {
      compact(render(JNull))
    } else {
      import org.json4s.JsonDSL._
      compact(render(value.toSeq.map {
        JString(_)
      }))
    }
  }

  override def jsonDecode(json: String): Array[String] = {
    parse(json) match {
      case JNull =>
        null
      case JArray(values) =>
        values.map {
          case JString(s) =>
            s
          case jValue =>
            throw new IllegalArgumentException(s"Cannot decode $jValue to String.")
        }.toArray
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Array[String].")
    }
  }
}
