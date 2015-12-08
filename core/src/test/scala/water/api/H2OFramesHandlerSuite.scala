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
import java.util

import com.google.gson.{JsonObject, JsonParser}
import org.apache.spark.SparkContext
import org.apache.spark.h2o._
import org.apache.spark.h2o.util.{SparkTestContext, SharedSparkTestContext}
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.{StructField, DataType, Metadata, StructType}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.DKV
import water.api.DataFrames.{H2OFrameIDV3, DataFrameV3, DataFramesV3, DataFramesHandler}
import water.api.H2OFrames.{H2OFramesHandler, DataFrameIDV3}
import water.api.RDDs.{RDDsV3, RDDsHandler}
import water.fvec.{Frame, H2OFrame}

import scala.util.parsing.json.{JSONObject, JSON}

/**
 * Test suite for h2oframes end-points
 */
@RunWith(classOf[JUnitRunner])
class H2OFramesHandlerSuite extends FunSuite with SparkTestContext {
  sc = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)
  hc = H2OContext.getOrCreate(sc)
  // Shared h2oContext
  val h2oContext = hc
  // Shared sqlContext
  implicit val sqlContext = new SQLContext(sc)

  test("H2OFramesHandler.toDataFrame() method"){
    // create H2OFrame which will be used for the transformation
    val h2oFrame = new H2OFrame(new File("./examples/smalldata/prostate.csv"))
    val h2OFramesHandler = new H2OFramesHandler(sc,h2oContext)

    val req = new DataFrameIDV3
    req.h2oframe_id = h2oFrame._key.toString
    val result = h2OFramesHandler.toDataFrame(3,req)

    // get the data frame using obtained id
    val df = sqlContext.table(result.dataframe_id)

    assert (result.msg.equals("Success"),"Status should be Success")
    assert (df.columns.size == h2oFrame.numCols(), "Number of columns should match")
    assert (df.columns.sameElements(h2oFrame.names()),"Column names should match")
    assert (df.count() == h2oFrame.numRows(), "Number of rows should match")
    assert (sqlContext.tableNames().size == 1, "Number of stored DataFrames should be 1")

  }

  test("H2OFramesHandler.toDataFrame() method, trying to convert H2OFrame which does not exist"){
    val h2OFramesHandler = new H2OFramesHandler(sc,h2oContext)
    val req = new DataFrameIDV3
    req.h2oframe_id = "does_not_exist"
    val result = h2OFramesHandler.toDataFrame(3,req)
    assert (result.dataframe_id.equals(""), "Returned H2O Frame id should be empty")
    assert (!result.msg.equals("OK"), "Status is not OK - it is message saying that given H2OFrame does not exist")
  }
}
