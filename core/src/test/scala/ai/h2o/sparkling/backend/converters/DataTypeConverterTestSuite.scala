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

package ai.h2o.sparkling.backend.converters

import ai.h2o.sparkling.SparkTestContext
import ai.h2o.sparkling.extensions.serde.ExpectedTypes
import ai.h2o.sparkling.extensions.serde.ExpectedTypes.ExpectedType
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.StringType
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner
import water.parser.Categorical

@RunWith(classOf[JUnitRunner])
class DataTypeConverterTestSuite extends FunSuite with SparkTestContext with Matchers {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  import spark.implicits._

  test("Columns with to many categorical level are be treated as strings") {
    val dataFrame = spark
      .range(Categorical.MAX_CATEGORICAL_COUNT * 4)
      .select(
        'id.cast(StringType) as "col1",
        ('id mod (Categorical.MAX_CATEGORICAL_COUNT * 2)).cast(StringType) as "col2",
        ('id mod (Categorical.MAX_CATEGORICAL_COUNT * 1.07)).cast(StringType) as "col3",
        ('id mod (Categorical.MAX_CATEGORICAL_COUNT)).cast(StringType) as "col4")

    val result = DataTypeConverter.determineExpectedTypes(dataFrame)

    result(0) shouldEqual ExpectedTypes.String
    result(1) shouldEqual ExpectedTypes.String
    result(2) shouldEqual ExpectedTypes.String
    result(3) shouldEqual ExpectedTypes.Categorical
  }

  test("Unique string columns are treated as strings, otherwise as categoricals") {
    val numberOfRows = 1000000
    val dataFrame = spark
      .range(numberOfRows)
      .select(
        'id.cast(StringType) as "col1",
        ('id mod (numberOfRows / 2)).cast(StringType) as "col2",
        ('id mod (numberOfRows / 4)).cast(StringType) as "col3")

    val result = DataTypeConverter.determineExpectedTypes(dataFrame)

    result(0) shouldEqual ExpectedTypes.String
    result(1) shouldEqual ExpectedTypes.Categorical
    result(2) shouldEqual ExpectedTypes.Categorical
  }
}
