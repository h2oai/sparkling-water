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

package water.sparkling.itest.local

import ai.h2o.sparkling.ml.utils.SchemaUtils
import ai.h2o.sparkling.utils.schemas.ComplexSchema
import org.apache.spark.h2o.utils.{SparkTestContext, TestFrameUtils}
import org.apache.spark.sql.Row
import org.apache.spark.{SparkConf, SparkContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

import scala.concurrent.duration._

@RunWith(classOf[JUnitRunner])
class H2OSchemaUtilsIntegrationTestSuite extends FunSuite with Matchers with SparkTestContext {
  val conf = new SparkConf()
    .set("spark.driver.extraClassPath", sys.props("java.class.path"))
    .set("spark.executor.extraClassPath", sys.props("java.class.path"))
    .set("spark.executor.memory", "1g")
    .set("spark.driver.memory", "2g")

  sc = new SparkContext("local-cluster[2,1,1024]", this.getClass.getSimpleName, conf)

  test("flattenDataFrame should process a complex data frame with more than 200k columns after flattening") {
    val expectedNumberOfColumns = 200000
    val settings = TestFrameUtils.GenerateDataFrameSettings(
      numberOfRows = 200,
      rowsPerPartition = 50,
      maxCollectionSize = 100)
    testFlatteningOnComplexType(settings, expectedNumberOfColumns)
  }

  test("flattenDataFrame should process a complex data frame with 100k rows and 2k columns") {
    val expectedNumberOfColumns = 2000
    val settings = TestFrameUtils.GenerateDataFrameSettings(
      numberOfRows = 100000,
      rowsPerPartition = 10000,
      maxCollectionSize = 10)
    testFlatteningOnComplexType(settings, expectedNumberOfColumns)
  }

  def testFlatteningOnComplexType(settings: TestFrameUtils.GenerateDataFrameSettings, expectedNumberOfColumns: Int) = {
    trackTime {
      val complexDF = TestFrameUtils.generateDataFrame(spark, ComplexSchema, settings)
      val flattened = SchemaUtils.flattenDataFrame(complexDF)

      val fieldTypeNames = flattened.schema.fields.map(_.dataType.typeName)
      val numberOfFields = fieldTypeNames.length
      println(s"Number of columns: $numberOfFields")
      numberOfFields shouldBe > (expectedNumberOfColumns)

      fieldTypeNames should contain noneOf("struct", "array", "map")
      flattened.foreach((r: Row) => r.toSeq.length)
    }
  }

  def trackTime[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // evaluate block
    val t1 = System.nanoTime()
    val diff = Duration.fromNanos(t1 - t0)
    println(s"Elapsed time: ${diff.toSeconds} seconds")
    result
  }
}
