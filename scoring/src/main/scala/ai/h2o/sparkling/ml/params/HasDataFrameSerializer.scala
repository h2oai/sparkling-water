package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.ml.models.H2OMOJOSettings
import org.apache.spark.ml.param.{Param, Params}

trait HasDataFrameSerializer extends Params {
  protected final val dataFrameSerializer = new Param[String](
    this,
    "dataFrameSerializer",
    "A full name of a serializer used for serialization and deserialization of Spark DataFrames " +
      "to a JSON value within NullableDataFrameParam.")

  setDefault(dataFrameSerializer -> H2OMOJOSettings.default.dataFrameSerializer)

  def getDataFrameSerializer(): String = $(dataFrameSerializer)

}
