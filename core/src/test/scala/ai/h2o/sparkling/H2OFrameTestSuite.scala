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
package ai.h2o.sparkling

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.{SharedH2OTestContext, TestFrameUtils}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.api.TestUtils

@RunWith(classOf[JUnitRunner])
class H2OFrameTestSuite extends FunSuite with SharedH2OTestContext {
  override def createSparkContext: SparkContext =
    new SparkContext("local[*]", getClass.getName, conf = defaultSparkConf)

  private def uploadH2OFrame(): H2OFrame = {
    // since we did not ask Spark to infer schema, all columns have been parsed as Strings
    val df = spark.read.option("header", "true").csv(TestUtils.locate("smalldata/prostate/prostate.csv"))
    H2OFrame(hc.asH2OFrameKeyString(df))
  }

  test("convertAllStringColumnsToCategorical") {
    val originalFrame = uploadH2OFrame()

    val stringColumns = originalFrame.columns.filter(_.dataType == H2OColumnType.string).map(_.name)
    assert(stringColumns.nonEmpty)
    val alteredFrame = originalFrame.convertAllStringColumnsToCategorical()
    val convertedColumns = alteredFrame.columns.filter(col => stringColumns.contains(col.name))

    // Verify that columns we asked to convert to categorical has been converted
    convertedColumns.foreach { col =>
      assert(col.dataType == H2OColumnType.`enum`)
    }
    // Check that all other columns remained unchanged
    alteredFrame.columns.diff(convertedColumns) foreach { col =>
      assert(col.dataType == originalFrame.columns.find(_.name == col.name).get.dataType)
    }
  }

  test("convertColumnsToCategorical with column names") {
    val originalFrame = uploadH2OFrame()
    val columnsToConvert = Array("ID", "AGE")
    val alteredFrame = originalFrame.convertColumnsToCategorical(columnsToConvert)
    val convertedColumns = alteredFrame.columns.filter(col => columnsToConvert.contains(col.name))
    // Verify that columns we asked to convert to categorical has been converted
    convertedColumns.foreach { col =>
      assert(col.dataType == H2OColumnType.`enum`)
    }
    // Check that all other columns remained unchanged
    alteredFrame.columns.diff(convertedColumns) foreach { col =>
      assert(col.dataType == originalFrame.columns.find(_.name == col.name).get.dataType)
    }
  }

  test("convertColumnsToCategorical with column indices") {
    val originalFrame = uploadH2OFrame()
    val columnsToConvert = Array(0, 1)
    val alteredFrame = originalFrame.convertColumnsToCategorical(columnsToConvert)
    val convertedColumns =
      alteredFrame.columns.zipWithIndex.filter(colInfo => columnsToConvert.contains(colInfo._2)).map(_._1)
    // Verify that columns we asked to convert to categorical has been converted
    convertedColumns.foreach { col =>
      assert(col.dataType == H2OColumnType.`enum`)
    }
    // Check that all other columns remained unchanged
    alteredFrame.columns.diff(convertedColumns) foreach { col =>
      assert(col.dataType == originalFrame.columns.find(_.name == col.name).get.dataType)
    }
  }

  test("split with ratio 1.0") {
    val originalFrame = uploadH2OFrame()
    val thrown = intercept[IllegalArgumentException] {
      originalFrame.split(1.0)
    }
    assert(thrown.getMessage == "Split ratios must be lower than 1.0")
  }

  test("split with ratio lower than 1.0") {
    val originalFrame = uploadH2OFrame()
    val splitFrames = originalFrame.split(0.9)
    assert(splitFrames.length == 2)
    val train = splitFrames(0)
    val valid = splitFrames(1)
    assert(train.numberOfRows == 342)
    assert(valid.numberOfRows == 38)
  }

  test("subframe with all columns") {
    val originalFrame = uploadH2OFrame()
    val newFrame = originalFrame.subframe(originalFrame.columns.map(_.name))
    assert(originalFrame == newFrame)
  }

  test("subframe with non-existent column") {
    val originalFrame = uploadH2OFrame()
    val nonExistentColumns = Array("non-existent-col")
    val thrown = intercept[IllegalArgumentException] {
      originalFrame.subframe(Array("non-existent-col"))
    }
    assert(
      thrown.getMessage == s"The following columns are not available on the H2OFrame ${originalFrame.frameId}: ${nonExistentColumns
        .mkString(", ")}")
  }

  test("subframe with specific columns") {
    val originalFrame = uploadH2OFrame()
    val selectedColumns = Array("AGE", "CAPSULE")
    val subframe = originalFrame.subframe(selectedColumns)
    assert(subframe.columnNames.sorted.sameElements(selectedColumns))
    assert(subframe.frameId != originalFrame.frameId)
  }

  test("joins") {
    import spark.implicits._
    val leftDf = sc.parallelize(Seq(("A", 12), ("B", 13), ("C", 14), ("D", 15))).toDF("name", "age")
    val rightDf = sc.parallelize(Seq(("Y", 10000), ("B", 20000), ("X", 30000), ("D", 40000))).toDF("name", "salary")
    val left = H2OFrame(hc.asH2OFrameKeyString(leftDf)).convertColumnsToCategorical(Array("name"))
    val right = H2OFrame(hc.asH2OFrameKeyString(rightDf)).convertColumnsToCategorical(Array("name"))

    type JoinFunc = (H2OFrame, String) => H2OFrame

    val testSpace: Array[(JoinFunc, Array[(String, Boolean)], Array[(String, Any, Any)])] =
      Array(
        (
          left.leftJoin,
          Array(("RADIX", true), ("HASH", true)),
          Array(("A", 12, null), ("B", 13, 20000), ("C", 14, null), ("D", 15, 40000))),
        (
          left.rightJoin,
          Array(("RADIX", false), ("HASH", true)),
          Array(("Y", null, 10000), ("B", 13, 20000), ("X", null, 10000), ("D", 15, 40000))),
        (left.innerJoin, Array(("RADIX", true), ("HASH", true)), Array(("B", 13, 20000), ("D", 15, 40000))),
        (
          left.outerJoin,
          Array(("RADIX", false), ("HASH", false)),
          Array(
            ("A", 12, null),
            ("B", 13, 20000),
            ("C", 14, null),
            ("D", 15, 40000),
            ("X", null, 10000),
            ("Y", null, 10000))))
    println(testSpace.mkString("\n"))

    for (testComb <- testSpace) {
      val (joinFunc, methods, expected) = testComb
      for (method <- methods) {
        val (joinMethod, enabled) = method
        if (enabled) {
          val result = joinFunc(right, joinMethod)
          assertFrameEqual("Data does not match", expected, result)
        }
      }
    }
  }

  test("join without categorical column") {}

  def assertFrameEqual(msg: String, expected: Seq[(String, Any, Any)], actual: H2OFrame): Unit = {
    assert(3 == actual.columns.length, s"""$msg: Number of columns has to match""")
    if (expected != null) {
      assert(expected.length == actual.numberOfRows, s"""$msg: Number of rows has to match""")
      val actualDF = hc.asDataFrame(actual.frameId).sort("name")
      import spark.implicits._
      val expectedDf = sc.parallelize(expected).toDF("name", "age", "salary").sort("name")
      TestFrameUtils.assertDataFramesAreIdentical(actualDF, expectedDf)
    }
  }
}
