package ai.h2o.sparkling.ml.params

import org.apache.spark.expose.Logging
import org.apache.spark.ml.param.{Param, Params}
import org.apache.spark.sql.DataFrame
import org.json4s.JsonAST.JNull
import org.json4s.jackson.JsonMethods.{compact, render}

class NonSerializableNullableDataFrameParam(
    parent: HasDataFrameSerializer,
    name: String,
    doc: String,
    isValid: DataFrame => Boolean)
  extends Param[DataFrame](parent, name, doc + " The parameter is not serializable!", isValid)
  with Logging {

  def this(parent: HasDataFrameSerializer, name: String, doc: String) =
    this(parent, name, doc, (_: DataFrame) => true)

  override def jsonEncode(dataFrame: DataFrame): String = {
    if (dataFrame != null) {
      logWarning(
        s"The parameter '$name' of the data frame type has been set, " +
          "but the value won't be serialized since the data frame can be potentially really big.")
    }
    compact(render(JNull))
  }

  override def jsonDecode(json: String): DataFrame = null
}
