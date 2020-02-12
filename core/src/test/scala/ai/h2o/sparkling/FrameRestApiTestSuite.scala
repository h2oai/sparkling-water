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

import ai.h2o.sparkling.frame.H2OColumnType
import org.apache.spark.SparkContext
import org.apache.spark.h2o.backends.external.RestApiUtils
import org.apache.spark.h2o.utils.SharedH2OTestContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.api.TestUtils

@RunWith(classOf[JUnitRunner])
class FrameRestApiTestSuite extends FunSuite with SharedH2OTestContext {
  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-local",
    conf = defaultSparkConf
      .set("spark.ext.h2o.rest.api.based.client", "true"))


  private def uploadH2OFrame(): String = {
    // since we did not ask Spark to infer schema, all columns have been parsed as Strings
    val df = spark.read.option("header", "true").csv(TestUtils.locate("smalldata/prostate/prostate.csv"))
    hc.asH2OFrameKeyString(df)
  }

  test("convertAllStringVecToCategorical") {
    val h2oFrameKey = uploadH2OFrame()
    val originalFrame = RestApiUtils.getFrame(hc.getConf, h2oFrameKey)

    val stringColumns = originalFrame.columns.filter(_.dataType == H2OColumnType.string).map(_.name)
    assert(stringColumns.nonEmpty)
    val alteredFrame = RestApiUtils.convertAllStringVecToCategorical(hc.getConf, h2oFrameKey)
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

  test("convertColumnsToCategorical") {
    val h2oFrameKey = uploadH2OFrame()
    val originalFrame = RestApiUtils.getFrame(hc.getConf, h2oFrameKey)
    val columnsToConvert = Array("ID", "AGE")
    val alteredFrame = RestApiUtils.convertColumnsToCategorical(hc.getConf, h2oFrameKey, columnsToConvert)
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

  test("splitFrameToTrainAndValidationFrames with ratio 1.0") {
    val h2oFrameKey = uploadH2OFrame()
    val thrown = intercept[IllegalArgumentException] {
      RestApiUtils.splitFrameToTrainAndValidationFrames(hc.getConf, h2oFrameKey, 1.0)
    }
    assert(thrown.getMessage == "Split ratio must be lower than 1.0")
  }

  test("splitFrameToTrainAndValidationFrames with ratio lower than 1.0") {
    val h2oFrameKey = uploadH2OFrame()
    val splitFrames = RestApiUtils.splitFrameToTrainAndValidationFrames(hc.getConf, h2oFrameKey, 0.9)
    assert(splitFrames.length == 2)
    val train = splitFrames(0)
    val valid = splitFrames(1)
    assert(train.numberOfRows == 342)
    assert(valid.numberOfRows == 38)
  }
}
