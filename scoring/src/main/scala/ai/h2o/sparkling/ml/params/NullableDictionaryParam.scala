package ai.h2o.sparkling.ml.params

import org.apache.spark.ml.param.{Param, Params}
import org.json4s.DefaultFormats
import org.json4s.JsonAST.JNull
import org.json4s.jackson.JsonMethods.{compact, render}
import org.json4s.jackson.Serialization.{read, write}

import scala.collection.JavaConverters._

class NullableDictionaryParam[T: Manifest](
    parent: Params,
    name: String,
    doc: String,
    isValid: java.util.Map[String, T] => Boolean)
  extends Param[java.util.Map[String, T]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, (_: java.util.Map[String, T]) => true)

  implicit def formats = DefaultFormats

  override def jsonEncode(dictionary: java.util.Map[String, T]): String = {
    if (dictionary == null) {
      compact(render(JNull))
    } else {
      write(dictionary.asScala)
    }
  }

  override def jsonDecode(json: String): java.util.Map[String, T] = {
    if (json == "null") {
      return null
    } else {
      read[Map[String, T]](json).asJava
    }
  }
}
