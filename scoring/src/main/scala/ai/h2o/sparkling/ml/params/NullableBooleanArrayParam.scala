package ai.h2o.sparkling.ml.params

import org.apache.spark.ml.param.{Param, ParamPair, Params}
import org.json4s.JNull
import org.json4s.JsonAST.{JArray, JBool}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import scala.collection.JavaConverters._

class NullableBooleanArrayParam(parent: Params, name: String, doc: String, isValid: Array[Boolean] => Boolean)
  extends Param[Array[Boolean]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[java.lang.Boolean]): ParamPair[Array[Boolean]] =
    w(value.asScala.map(_.asInstanceOf[Boolean]).toArray)

  override def jsonEncode(value: Array[Boolean]): String = {
    if (value == null) {
      compact(render(JNull))
    } else {
      import org.json4s.JsonDSL._
      compact(render(value.toSeq.map(JBool(_))))
    }
  }

  override def jsonDecode(json: String): Array[Boolean] = {
    parse(json) match {
      case JNull =>
        null
      case JArray(values) =>
        values.map {
          case JBool(x) => x
          case jValue => throw new IllegalArgumentException(s"Cannot decode $jValue to Boolean.")
        }.toArray
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Array[Boolean].")
    }
  }
}
