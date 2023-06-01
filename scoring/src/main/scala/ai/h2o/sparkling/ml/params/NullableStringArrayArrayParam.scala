package ai.h2o.sparkling.ml.params

import org.apache.spark.ml.param.{Param, ParamPair, Params}
import org.json4s.JsonAST.{JArray, JInt}
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{JNull, JValue}
import water.AutoBuffer

import scala.collection.JavaConverters._

class NullableStringArrayArrayParam(parent: Params, name: String, doc: String, isValid: Array[Array[String]] => Boolean)
  extends Param[Array[Array[String]]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[java.util.List[java.lang.String]]): ParamPair[Array[Array[String]]] = {
    w(value.asScala.map(array => array.asScala.toArray).toArray)
  }

  override def jsonEncode(value: Array[Array[String]]): String = {
    val encoded: JValue = if (value == null) {
      JNull
    } else {
      val ab = new AutoBuffer()
      ab.putAAStr(value)
      val bytes = ab.buf()
      JArray(bytes.toSeq.map(JInt(_)).toList)
    }
    compact(render(encoded))
  }

  override def jsonDecode(json: String): Array[Array[String]] = {
    parse(json) match {
      case JNull =>
        null
      case JArray(values) =>
        val bytes = values.map {
          case JInt(x) =>
            x.byteValue()
          case _ =>
            throw new IllegalArgumentException(s"Cannot decode $json to Byte.")
        }.toArray
        val ab = new AutoBuffer(bytes)
        ab.getAAStr()
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Array[Array[String]].")
    }
  }
}
