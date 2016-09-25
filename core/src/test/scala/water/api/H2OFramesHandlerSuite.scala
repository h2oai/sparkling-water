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
package water.api

import java.io.File

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SharedSparkTestContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.api.H2OFrames.{DataFrameIDV3, H2OFramesHandler}
import water.exceptions.H2ONotFoundArgumentException
import water.fvec.H2OFrame

/**
 * Test suite for H2OFrames handler
 */
@RunWith(classOf[JUnitRunner])
class H2OFramesHandlerSuite extends FunSuite with SharedSparkTestContext {
  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)

  test("H2OFramesHandler.toDataFrame() method"){
    // create H2OFrame which will be used for the transformation
    val h2oFrame = new H2OFrame(new File("./examples/smalldata/prostate.csv"))
    val h2oFramesHandler = new H2OFramesHandler(sc,hc)

    val req = new DataFrameIDV3
    req.h2oframe_id = h2oFrame._key.toString
    req.dataframe_id = "requested_name"
    val result = h2oFramesHandler.toDataFrame(3,req)

    // get the data frame using obtained id
    val df = sqlContext.table(result.dataframe_id)
    assert (sqlContext.tableNames().contains("requested_name"), "DataFrame should be stored in table named \"requested_name\"")
    assert (df.columns.length == h2oFrame.numCols(), "Number of columns should match")
    assert (df.columns.sameElements(h2oFrame.names()),"Column names should match")
    assert (df.count() == h2oFrame.numRows(), "Number of rows should match")
    // Note: We need to be careful here and clean SparkSession properly
    assert (sqlContext.tableNames().length == 1, "Number of stored DataFrames should be 1")

  }

  test("H2OFramesHandler.toDataFrame() method, trying to convert H2OFrame which does not exist"){
    val h2oFramesHandler = new H2OFramesHandler(sc,hc)
    val req = new DataFrameIDV3
    req.h2oframe_id = "does_not_exist"
    intercept[H2ONotFoundArgumentException] {
      h2oFramesHandler.toDataFrame(3,req)
    }
  }
}
