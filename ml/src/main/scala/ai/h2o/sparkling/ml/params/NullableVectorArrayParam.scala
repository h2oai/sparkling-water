package ai.h2o.sparkling.ml.params

import org.apache.spark.ml.param.{Param, ParamPair, Params}
import org.apache.spark.ml.linalg.DenseVector
import org.json4s.JsonAST.{JArray, JNull}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import scala.collection.JavaConverters._

class NullableVectorArrayParam(parent: Params, name: String, doc: String, isValid: Array[DenseVector] => Boolean)
  extends Param[Array[DenseVector]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[DenseVector]): ParamPair[Array[DenseVector]] = w(value.asScala.toArray)

  override def jsonEncode(vectors: Array[DenseVector]): String = {
    val ast = if (vectors == null) {
      JNull
    } else {
      val astVectors = vectors.map(vector => JArray(vector.values.map(v => DoubleParam.jValueEncode(v)).toList))
      JArray(astVectors.toList)
    }
    compact(render(ast))
  }

  override def jsonDecode(json: String): Array[DenseVector] = {
    parse(json) match {
      case JNull =>
        null
      case JArray(values) =>
        values.map {
          case JArray(v) => new DenseVector(v.map(DoubleParam.jValueDecode).toArray)
          case _ => throw new IllegalArgumentException(s"Cannot decode $json to DenseVector.")
        }.toArray
      case _ => throw new IllegalArgumentException(s"Cannot decode $json to Array[DenseVector].")
    }
  }
}
