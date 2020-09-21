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

import java.io.File
import java.net.URI

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class H2OFrameTestSuite extends FunSuite with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")
  import spark.implicits._

  private def uploadH2OFrame(): H2OFrame = H2OFrame(new File(TestUtils.locate("smalldata/prostate/prostate.csv")))

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

  test("convertColumnsToStrings with column names") {
    val columnsToConvert = Array("ID", "AGE")
    val originalFrame = uploadH2OFrame().convertColumnsToCategorical(columnsToConvert)
    val alteredFrame = originalFrame.convertColumnsToStrings(columnsToConvert)
    val convertedColumns = alteredFrame.columns.filter(col => columnsToConvert.contains(col.name))
    // Verify that columns we asked to convert to categorical has been converted
    convertedColumns.foreach { col =>
      assert(col.dataType == H2OColumnType.string)
    }
    // Check that all other columns remained unchanged
    alteredFrame.columns.diff(convertedColumns) foreach { col =>
      assert(col.dataType == originalFrame.columns.find(_.name == col.name).get.dataType)
    }
  }

  test("convertColumnsToStrings with column indices") {
    val columnsToConvert = Array(0, 1)
    val originalFrame = uploadH2OFrame().convertColumnsToCategorical(columnsToConvert)
    val alteredFrame = originalFrame.convertColumnsToStrings(columnsToConvert)
    val convertedColumns =
      alteredFrame.columns.zipWithIndex.filter(colInfo => columnsToConvert.contains(colInfo._2)).map(_._1)
    // Verify that columns we asked to convert to categorical has been converted
    convertedColumns.foreach { col =>
      assert(col.dataType == H2OColumnType.string)
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

  test("delete") {
    val originalFrame = uploadH2OFrame()
    originalFrame.delete()
    val frames = H2OFrame.listFrames()
    assert(!frames.map(_.frameId).contains(originalFrame.frameId))
  }

  test("add") {
    val originalFrame = uploadH2OFrame()
    val newFrame = originalFrame.add(originalFrame)
    val newNames = originalFrame.columnNames ++ originalFrame.columnNames.map(_ + "0")
    assert(newFrame.columnNames.sameElements(newNames))
  }

  private lazy val leftFrame = {
    val leftDf = sc
      .parallelize(Seq(("A", 12, "NYC"), ("B", 13, "SF"), ("C", 14, "PRG"), ("D", 15, "SYD")))
      .toDF("name", "age", "city")
    hc.asH2OFrame(leftDf).convertColumnsToCategorical(Array("name"))
  }

  private lazy val rightFrame = {
    val rightDf = sc.parallelize(Seq(("Y", 10000), ("B", 20000), ("X", 30000), ("D", 40000))).toDF("name", "salary")
    hc.asH2OFrame(rightDf).convertColumnsToCategorical(Array("name"))
  }

  test("Left join") {
    val expected = sc
      .parallelize(
        Seq(
          ("A", 12, "NYC", None),
          ("B", 13, "SF", Some(20000)),
          ("C", 14, "PRG", None),
          ("D", 15, "SYD", Some(40000))))
      .toDF("name", "age", "city", "salary")

    val result = leftFrame.leftJoin(rightFrame)
    assertAfterJoin(result, expected)
  }

  test("Right join") {
    val expected = sc
      .parallelize(
        Seq(
          ("Y", None, None, 10000),
          ("B", Some(13), Some("SF"), 20000),
          ("X", None, None, 30000),
          ("D", Some(15), Some("SYD"), 40000)))
      .toDF("name", "age", "city", "salary")

    val result = leftFrame.rightJoin(rightFrame)
    assertAfterJoin(result, expected)
  }

  test("Inner join") {
    val expected =
      sc.parallelize(Seq(("B", 13, "SF", 20000), ("D", 15, "SYD", 40000))).toDF("name", "age", "city", "salary")

    val result = leftFrame.innerJoin(rightFrame)
    assertAfterJoin(result, expected)
  }

  test("Outer join") {
    val expected = sc
      .parallelize(
        Seq(
          ("A", Some(12), Some("NYC"), None),
          ("B", Some(13), Some("SF"), Some(20000)),
          ("C", Some(14), Some("PRG"), None),
          ("D", Some(15), Some("SYD"), Some(40000)),
          ("X", None, None, Some(30000)),
          ("Y", None, None, Some(10000))))
      .toDF("name", "age", "city", "salary")

    val result = leftFrame.outerJoin(rightFrame)
    assertAfterJoin(result, expected)
  }

  test("Create h2o frame from https resource") {
    val fr = H2OFrame(
      new URI(
        "https://raw.githubusercontent.com/h2oai/sparkling-water/master/examples/smalldata/prostate/prostate.csv"))
    assert(fr.numberOfRows == 380)
    assert(fr.numberOfColumns == 9)
  }

  test("Create h2o frame from file resource") {
    val fr = H2OFrame(new URI("file://" + TestUtils.locate("smalldata/prostate/prostate.csv")))
    assert(fr.numberOfRows == 380)
    assert(fr.numberOfColumns == 9)
  }

  test("Rename non-existent column") {
    val fr = H2OFrame(new URI("file://" + TestUtils.locate("smalldata/prostate/prostate.csv")))
    val thrown = intercept[IllegalArgumentException] {
      fr.renameCol("i_do_not_exist", "new_col_name")
    }
    assert(thrown.getMessage == s"Column 'i_do_not_exist' does not exist in the frame ${fr.frameId}")
  }

  test("Rename existing column") {
    val fr = H2OFrame(new URI("file://" + TestUtils.locate("smalldata/prostate/prostate.csv"))).subframe(Array("AGE"))
    val originalLength = fr.columnNames.length
    fr.renameCol("AGE", "NEW_AGE")
    assert(!fr.columnNames.contains("AGE"))
    assert(fr.columnNames.contains("NEW_AGE"))
    assert(fr.columnNames.length == originalLength)
  }

  private def assertAfterJoin(result: H2OFrame, expected: DataFrame): Unit = {
    val resultDF = hc.asSparkFrame(result.frameId).select("name", "age", "city", "salary")
    TestUtils.assertDataFramesAreIdentical(resultDF, expected)
  }
}
