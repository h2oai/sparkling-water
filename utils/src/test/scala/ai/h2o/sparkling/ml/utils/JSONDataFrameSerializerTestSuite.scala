package ai.h2o.sparkling.ml.utils

import ai.h2o.sparkling.TestUtils
import ai.h2o.sparkling.backend.converters.DataFrameConversionSuiteBase
import ai.h2o.sparkling.utils.JSONDataFrameSerializer
import org.apache.spark.sql.Row
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.apache.spark.sql.types.StructType

class JSONDataFrameSerializerTestSuite extends DataFrameConversionSuiteBase {

  (simpleColumns ++ simpleColumnsWithNulls).foreach { columnSpec =>
    test(s"Convert DataFrame[${columnSpec.field.name}] of various types to Json string and back") {
      val serializer = new JSONDataFrameSerializer()
      val numberOfRows = 200
      val rows = columnSpec.valueGenerator(numberOfRows).map(Row(_))
      val rdd = spark.sparkContext.parallelize(rows, 4)
      val dataFrame = spark.createDataFrame(rdd, StructType(columnSpec.field :: Nil))

      val serialized = compact(render(serializer.serialize(dataFrame)))
      val deserialized = serializer.deserialize(parse(serialized))

      TestUtils.assertDataFramesAreIdentical(dataFrame, deserialized)
    }
  }
}
