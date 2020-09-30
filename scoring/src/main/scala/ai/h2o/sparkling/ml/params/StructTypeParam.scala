package ai.h2o.sparkling.ml.params

import org.apache.spark.ml.param.{Param, ParamPair, Params}
import org.apache.spark.sql.types.{DataType, StructType}

class StructTypeParam(parent: Params, name: String, doc: String, isValid: StructType => Boolean)
  extends Param[StructType](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  override def w(value: StructType): ParamPair[StructType] = super.w(value)

  override def jsonEncode(value: StructType): String = {
    value.json
  }

  override def jsonDecode(json: String): StructType = {
    DataType.fromJson(json) match {
      case t: StructType => t
      case _ => throw new RuntimeException(s"Failed parsing ${json} as StructType")
    }
  }
}
