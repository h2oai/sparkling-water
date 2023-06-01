package ai.h2o.sparkling.ml.params

import org.apache.spark.ml.linalg.DenseMatrix
import org.apache.spark.ml.param.{Param, ParamPair, Params}
import org.json4s.JsonAST._
import org.json4s.jackson.JsonMethods.{compact, parse, render}

import scala.collection.JavaConverters._

class NullableMatrixArrayParam(parent: Params, name: String, doc: String, isValid: Array[DenseMatrix] => Boolean)
  extends Param[Array[DenseMatrix]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[DenseMatrix]): ParamPair[Array[DenseMatrix]] = w(value.asScala.toArray)

  override def jsonEncode(value: Array[DenseMatrix]): String = {
    val ast = if (value == null) {
      JNull
    } else {
      val matrixObjects = value.map { matrix =>
        val numRows = JField("numRows", JInt(matrix.numRows))
        val numCols = JField("numCols", JInt(matrix.numRows))
        val valueArray = JArray(matrix.values.map(DoubleParam.jValueEncode).toList)
        val values = JField("values", valueArray)
        val transposed = JField("transposed", JBool(matrix.isTransposed))
        JObject(numRows, numCols, values, transposed)
      }.toList
      JArray(matrixObjects)
    }
    compact(render(ast))
  }

  override def jsonDecode(json: String): Array[DenseMatrix] = {
    parse(json) match {
      case JNull =>
        null
      case JArray(matrices) =>
        matrices.map {
          case JObject(
              List(
                JField("numRows", JInt(numRows)),
                JField("numCols", JInt(numCols)),
                JField("values", JArray(valueArray)),
                JField("transposed", JBool(transposed)))) =>
            val values = valueArray.map(DoubleParam.jValueDecode).toArray
            new DenseMatrix(numRows.toInt, numCols.toInt, values, transposed)
          case _ =>
            throw new IllegalArgumentException(s"Cannot decode $json to DenseMatrix.")
        }.toArray
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Array[DenseMatrix].")
    }
  }
}
