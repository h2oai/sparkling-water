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

package ai.h2o.sparkling.ml

import ai.h2o.sparkling.ml.utils.SchemaUtils
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.SparkContext
import org.apache.spark.sql.Row
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class IntegrationTestSuite extends FunSuite with SharedH2OTestContext {

  override def createSparkContext: SparkContext =
    new SparkContext("local-cluster[2,1,2024]", getClass.getName, conf = defaultSparkConf)

  test("SchemaUtils: flattenDataFrame should process a complex data frame with more than 200k columns after flattening") {
    val expectedNumberOfColumns = 200000
    val settings =
      TestUtils.GenerateDataFrameSettings(numberOfRows = 200, rowsPerPartition = 50, maxCollectionSize = 100)
    testFlatteningOnComplexType(settings, expectedNumberOfColumns)
  }

  test("SchemaUtils: flattenDataFrame should process a complex data frame with 100k rows and 2k columns") {
    val expectedNumberOfColumns = 2000
    val settings =
      TestUtils.GenerateDataFrameSettings(numberOfRows = 100000, rowsPerPartition = 10000, maxCollectionSize = 10)
    testFlatteningOnComplexType(settings, expectedNumberOfColumns)
  }

  private def testFlatteningOnComplexType(
      settings: TestUtils.GenerateDataFrameSettings,
      expectedNumberOfColumns: Int): Unit = {
    trackTime {
      val complexDF = TestUtils.generateDataFrame(spark, ComplexSchema, settings)
      val flattened = SchemaUtils.flattenDataFrame(complexDF)

      val fieldTypeNames = flattened.schema.fields.map(_.dataType.typeName)
      val numberOfFields = fieldTypeNames.length
      println(s"Number of columns: $numberOfFields")
      assert(numberOfFields > expectedNumberOfColumns)
      assert(fieldTypeNames.intersect(Array("struct", "array", "map")).isEmpty)
      flattened.foreach((r: Row) => r.toSeq.length)
    }
  }

  private def trackTime[R](block: => R): R = {
    val t0 = System.nanoTime()
    val result = block // evaluate block
    val t1 = System.nanoTime()
    val diff = Duration.fromNanos(t1 - t0)
    println(s"Elapsed time: ${diff.toSeconds} seconds")
    result
  }
}
