/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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
