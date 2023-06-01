package ai.h2o.sparkling.utils

import org.apache.spark.sql.DataFrame
import org.json4s.JsonAST.JValue

trait DataFrameSerializer {
  def serialize(df: DataFrame): JValue

  def deserialize(input: JValue): DataFrame
}

object DataFrameSerializer {
  def default = new JSONDataFrameSerializer()
}
