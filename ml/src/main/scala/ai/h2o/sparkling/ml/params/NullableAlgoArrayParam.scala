package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.ml.algos._
import hex.Model
import org.apache.spark.ml.param.{Param, ParamPair, Params}
import org.json4s.JsonAST.JNull
import org.json4s._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import scala.collection.JavaConverters._

class NullableAlgoArrayParam(
    parent: Params,
    name: String,
    doc: String,
    isValid: Array[H2OAlgorithm[_ <: Model.Parameters]] => Boolean)
  extends Param[Array[H2OAlgorithm[_ <: Model.Parameters]]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[H2OAlgorithm[_ <: Model.Parameters]])
      : ParamPair[Array[H2OAlgorithm[_ <: Model.Parameters]]] = {
    w(value.asScala.toArray)
  }

  override def jsonEncode(algos: Array[H2OAlgorithm[_ <: Model.Parameters]]): String = {
    val encoded = if (algos == null) {
      JNull
    } else {
      JArray(algos.toList.map(AlgoParam.jsonEncode(_)))
    }
    compact(render(encoded))
  }

  override def jsonDecode(json: String): Array[H2OAlgorithm[_ <: Model.Parameters]] = {
    val parsed = parse(json)

    parsed match {
      case JNull =>
        null
      case JArray(values) =>
        values.map {
          AlgoParam.jsonDecode(_)
        }.toArray
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Array[H2OAlgorithm[...]].")
    }
  }
}
