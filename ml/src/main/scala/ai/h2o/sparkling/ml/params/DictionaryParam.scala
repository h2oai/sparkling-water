package ai.h2o.sparkling.ml.params

import org.apache.spark.ml.param.{Param, Params}
import org.json4s.DefaultFormats
import org.json4s.jackson.Serialization.{read, write}

import scala.collection.JavaConverters._

class DictionaryParam(parent: Params, name: String, doc: String, isValid: java.util.Map[String, Double] => Boolean)
  extends Param[java.util.Map[String, Double]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  implicit def formats = DefaultFormats

  override def jsonEncode(dictionary: java.util.Map[String, Double]): String = write(dictionary.asScala)

  override def jsonDecode(json: String): java.util.Map[String, Double] = read[Map[String, Double]](json).asJava
}
