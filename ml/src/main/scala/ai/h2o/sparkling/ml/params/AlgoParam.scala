package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.ml.algos._
import hex.Model
import org.apache.spark.ml.param.{Param, ParamPair, Params}
import org.json4s.JsonAST.JNull
import org.json4s.JsonDSL._
import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

class AlgoParam(parent: Params, name: String, doc: String, isValid: H2OAlgorithm[_ <: Model.Parameters] => Boolean)
  extends Param[H2OAlgorithm[_ <: Model.Parameters]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  override def jsonEncode(value: H2OAlgorithm[_ <: Model.Parameters]): String = {
    val encoded = AlgoParam.jsonEncode(value)
    compact(render(encoded))
  }

  override def jsonDecode(json: String): H2OAlgorithm[_ <: Model.Parameters] = {
    val parsed = parse(json)
    AlgoParam.jsonDecode(parsed)
  }
}

object AlgoParam {

  def jsonEncode(value: H2OAlgorithm[_ <: Model.Parameters]): JValue = {
    if (value == null) {
      JNull
    } else {
      val algoClassName = value.getClass.getName
      val uid = value.uid
      val params = value.extractParamMap().toSeq
      val jsonParams = render(params.map {
        case ParamPair(p, v) =>
          p.name -> parse(p.jsonEncode(v))
      }.toList)

      ("class" -> algoClassName) ~ ("uid" -> uid) ~ ("paramMap" -> jsonParams)
    }
  }

  def jsonDecode(parsed: JValue): H2OAlgorithm[_ <: Model.Parameters] = {
    if (parsed == JNull) {
      null
    } else {
      implicit val format = DefaultFormats
      val className = (parsed \ "class").extract[String]
      val uid = (parsed \ "uid").extract[String]
      val jsonParams = parsed \ "paramMap"

      val algo = createH2OAlgoInstance(className, uid)
      jsonParams match {
        case JObject(pairs) =>
          pairs.foreach {
            case (paramName, jsonValue) =>
              val param = algo.getParam(paramName)
              val value = param.jsonDecode(compact(render(jsonValue)))
              algo.set(param, value)
          }
        case _ => throw new RuntimeException("Invalid JSON parameters")
      }
      algo
    }
  }

  private def createH2OAlgoInstance(algoName: String, uid: String): H2OAlgorithm[_ <: Model.Parameters] = {
    val cls = Class.forName(algoName)
    cls.getConstructor(classOf[String]).newInstance(uid).asInstanceOf[H2OAlgorithm[_ <: Model.Parameters]]
  }
}
