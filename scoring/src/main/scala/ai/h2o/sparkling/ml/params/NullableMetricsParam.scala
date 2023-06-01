package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.ml.metrics.H2OMetrics
import ai.h2o.sparkling.ml.utils.H2OParamsReader
import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.ml.util.expose.DefaultParamsReader.Metadata
import org.json4s._
import org.json4s.JsonDSL._
import org.json4s.JsonAST.JNull
import org.json4s.jackson.JsonMethods.{compact, parse, render}

class NullableMetricsParam(parent: Params, name: String, doc: String, isValid: H2OMetrics => Boolean)
  extends Param[H2OMetrics](parent, name, doc, isValid)
  with H2OParamsReader[H2OMetrics] {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, (_: H2OMetrics) => true)

  override def jsonEncode(metrics: H2OMetrics): String = {
    val ast = if (metrics == null) {
      JNull
    } else {
      val params = metrics.params.filter(p => metrics.isSet(p)).map { p =>
        metrics.getParam(p.name)
      }
      val jsonParams = render(params.map { p =>
        p.name -> parse(p.jsonEncode(metrics.getOrDefault(p)))
      }.toList)
      val metadataAst = ("class" -> metrics.getClass.getName) ~ ("uid" -> metrics.uid) ~ ("paramMap" -> jsonParams)
      metadataAst
    }
    compact(render(ast))
  }

  override def jsonDecode(json: String): H2OMetrics = {
    parse(json) match {
      case JNull =>
        null
      case ast =>
        implicit val format = DefaultFormats
        val className = (ast \ "class").extract[String]
        val uid = (ast \ "uid").extract[String]
        val params = ast \ "paramMap"
        val metadata = Metadata(className, uid, params, json)
        load(metadata)
    }
  }
}
