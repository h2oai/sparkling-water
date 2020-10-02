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
    myTest(
      Categorical.MAX_CATEGORICAL_COUNT / 10,
      (ExpectedTypes.String, ExpectedTypes.Categorical, ExpectedTypes.Categorical))
  }

  def myTest(number: Int, expectedColumnTypes: (ExpectedType, ExpectedType, ExpectedType)): Unit = {
    val n = (number * 1.07).asInstanceOf[Int]
    val dataframe = spark
      .range(number * 4)
      .select(
        'id.cast(StringType) as "col1",
        ('id mod (number * 2)).cast(StringType) as "col2",
        ('id mod n).cast(StringType) as "col3")
    dataframe.show()

    val result = DataTypeConverter.determineExpectedTypes(dataframe)

    result(0) shouldEqual expectedColumnTypes._1
    result(1) shouldEqual expectedColumnTypes._2
    result(2) shouldEqual expectedColumnTypes._3
  }
}
