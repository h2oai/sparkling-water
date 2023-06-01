package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.utils.DataFrameSerializationWrappers._
import ai.h2o.sparkling.utils.{DataFrameJsonSerialization, DataFrameSerializer}
import org.apache.spark.ml.param.{Param, ParamPair}
import org.apache.spark.sql.DataFrame
import scala.collection.JavaConverters._

class NullableDataFrameArrayParam(
    parent: HasDataFrameSerializer,
    name: String,
    doc: String,
    isValid: DataFrameArraySerializationWrapper => Boolean)
  extends Param[DataFrameArraySerializationWrapper](parent, name, doc, isValid) {

  def this(parent: HasDataFrameSerializer, name: String, doc: String) =
    this(parent, name, doc, _ => true)

  def w(value: java.util.List[DataFrame]): ParamPair[DataFrameArraySerializationWrapper] = {
    w(toWrapper(value.asScala.toArray))
  }

  override def jsonEncode(dataFrames: DataFrameArraySerializationWrapper): String = {
    val serializerClassName = parent.getDataFrameSerializer()
    val serializer = Class.forName(serializerClassName).newInstance().asInstanceOf[DataFrameSerializer]
    DataFrameJsonSerialization.encodeDataFrames(dataFrames, serializer)
  }

  override def jsonDecode(json: String): DataFrameArraySerializationWrapper = {
    DataFrameJsonSerialization.decodeDataFrames(json)
  }
}
