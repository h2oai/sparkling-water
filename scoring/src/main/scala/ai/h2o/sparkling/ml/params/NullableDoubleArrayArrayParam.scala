package ai.h2o.sparkling.ml.params

import org.apache.spark.ml.param.{Param, ParamPair, Params}
import org.json4s.JsonAST.{JArray, JNull}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import scala.collection.JavaConverters._

class NullableDoubleArrayArrayParam(parent: Params, name: String, doc: String, isValid: Array[Array[Double]] => Boolean)
  extends Param[Array[Array[Double]]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[java.util.List[java.lang.Double]]): ParamPair[Array[Array[Double]]] =
    w(value.asScala.map(_.asScala.map(_.asInstanceOf[Double]).toArray).toArray)

  override def jsonEncode(value: Array[Array[Double]]): String = {
    if (value == null) {
      compact(render(JNull))
    } else {
      import org.json4s.JsonDSL._
      compact(render(value.toSeq.map(_.toSeq.map(v => DoubleParam.jValueEncode(v)))))
    }
  }

  override def jsonDecode(json: String): Array[Array[Double]] = {
    parse(json) match {
      case JNull =>
        null
      case JArray(values) =>
        values.map {
          case JArray(v) =>
            v.map(DoubleParam.jValueDecode).toArray
          case _ =>
            throw new IllegalArgumentException(s"Cannot decode $json to Array[Double].")
        }.toArray
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Array[Array[Double]].")
    }
  }
}
