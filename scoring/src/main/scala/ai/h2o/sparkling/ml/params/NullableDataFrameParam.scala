package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.utils.DataFrameSerializationWrappers._
import ai.h2o.sparkling.utils.{DataFrameJsonSerialization, DataFrameSerializer}
import org.apache.spark.ml.param.{Param, ParamPair}
import org.apache.spark.sql.DataFrame

class NullableDataFrameParam(
    parent: HasDataFrameSerializer,
    name: String,
    doc: String,
    isValid: DataFrameSerializationWrapper => Boolean)
  extends Param[DataFrameSerializationWrapper](parent, name, doc, isValid) {

  def this(parent: HasDataFrameSerializer, name: String, doc: String) = this(parent, name, doc, _ => true)

  def w(value: DataFrame): ParamPair[DataFrameSerializationWrapper] = w(toWrapper(value))

  override def jsonEncode(dataFrameWrapper: DataFrameSerializationWrapper): String = {
    val serializerClassName = parent.getDataFrameSerializer()
    val serializer = Class.forName(serializerClassName).newInstance().asInstanceOf[DataFrameSerializer]
    DataFrameJsonSerialization.encodeDataFrame(dataFrameWrapper, serializer)
  }

  override def jsonDecode(json: String): DataFrameSerializationWrapper =
    DataFrameJsonSerialization.decodeDataFrame(json)
}
