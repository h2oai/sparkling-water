package ai.h2o.sparkling.ml.params

import org.apache.spark.ml.param.{Param, ParamPair, Params}
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{JDouble, JNull, JString}

import scala.collection.JavaConverters._

class NullableFloatArrayParam(parent: Params, name: String, doc: String, isValid: Array[Float] => Boolean)
  extends Param[Array[Float]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[java.lang.Double]): ParamPair[Array[Float]] =
    w(value.asScala.map(_.doubleValue().toFloat).toArray)

  override def jsonEncode(value: Array[Float]): String = {
    if (value == null) {
      compact(render(JNull))
    } else {
      import org.json4s.JsonDSL._
      compact(render(value.toSeq.map {
        case v if v.isNaN =>
          JString("NaN")
        case Float.NegativeInfinity =>
          JString("-Inf")
        case Float.PositiveInfinity =>
          JString("Inf")
        case v =>
          JDouble(v)
      }))
    }
  }

  override def jsonDecode(json: String): Array[Float] = {
    parse(json) match {
      case JNull =>
        null
      case JArray(values) =>
        values.map {
          case JString("NaN") =>
            Float.NaN
          case JString("-Inf") =>
            Float.NegativeInfinity
          case JString("Inf") =>
            Float.PositiveInfinity
          case JDouble(x) =>
            x.toFloat
          case jValue =>
            throw new IllegalArgumentException(s"Cannot decode $jValue to Float.")
        }.toArray
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Array[Float].")
    }
  }
}
