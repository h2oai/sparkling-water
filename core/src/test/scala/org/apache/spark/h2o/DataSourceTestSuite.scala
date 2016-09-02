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

package org.apache.spark.h2o

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SharedSparkTestContext
import org.apache.spark.sql.SaveMode
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.DKV
import testdata._

/**
  * Test using H2O Frame as Spark SQL data source
  */
@RunWith(classOf[JUnitRunner])
class DataSourceTestSuite extends FunSuite with SharedSparkTestContext {

  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-data-sources",
    conf = defaultSparkConf)
  
  test("Reading H2OFrame using short variant") {
    val rdd = sc.parallelize(1 to 1000).map( v => IntHolder(Some(v)))
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    val df = sqlContext.read.h2o(h2oFrame.key)

    assert (df.columns.length == h2oFrame.numCols(), "Number of columns should match")
    assert (df.columns.sameElements(h2oFrame.names()),"Column names should match")
    assert (df.count() == h2oFrame.numRows(), "Number of rows should match")
  }

  test("Reading H2OFrame using key option") {
    val rdd = sc.parallelize(1 to 1000).map( v => IntHolder(Some(v)))
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    val df = sqlContext.read.format("h2o").option("key", h2oFrame.key.toString).load()

    assert (df.columns.length == h2oFrame.numCols(), "Number of columns should match")
    assert (df.columns.sameElements(h2oFrame.names()),"Column names should match")
    assert (df.count() == h2oFrame.numRows(), "Number of rows should match")
  }

  test("Reading H2OFrame using key in load method ") {
    val rdd = sc.parallelize(1 to 1000).map( v => IntHolder(Some(v)))
    val h2oFrame: H2OFrame = hc.asH2OFrame(rdd)
    val df = sqlContext.read.format("h2o").load(h2oFrame.key.toString)

    assert (df.columns.length == h2oFrame.numCols(), "Number of columns should match")
    assert (df.columns.sameElements(h2oFrame.names()),"Column names should match")
    assert (df.count() == h2oFrame.numRows(), "Number of rows should match")
  }

  test("Writing DataFrame to new H2O Frame ") {
    val rdd = sc.parallelize(1 to 1000).map( v => IntHolder(Some(v)))
    val df = sqlContext.createDataFrame(rdd)
    df.write.h2o("new_key")

    val h2oFrame = DKV.getGet[Frame]("new_key")
    assert (df.columns.length == h2oFrame.numCols(), "Number of columns should match")
    assert (df.columns.sameElements(h2oFrame.names()),"Column names should match")
    assert (df.count() == h2oFrame.numRows(), "Number of rows should match")
    h2oFrame.remove()
  }


  test("Writing DataFrame to existing H2O Frame ") {
    val rdd = sc.parallelize(1 to 1000).map( v => IntHolder(Some(v)))
    val df = sqlContext.createDataFrame(rdd)
    df.write.h2o("new_key")

    val rddNew = sc.parallelize(1 to 1000).map( v => StringHolder(Some(v.toString)))
    val dfNew = sqlContext.createDataFrame(rddNew)

    val h2oFrame = DKV.getGet[Frame]("new_key")
    val thrown = intercept[RuntimeException] {
      dfNew.write.format("h2o").mode(SaveMode.ErrorIfExists).save("new_key")
    }

    assert(thrown.getMessage == "Frame with key 'new_key' already exists, if you want to override it set the save mode to override.")
    h2oFrame.remove()
  }

  test("Overwriting existing H2O Frame ") {
    val rdd = sc.parallelize(1 to 1000).map( v => IntHolder(Some(v)))
    val df = sqlContext.createDataFrame(rdd)
    df.write.h2o("new_key")

    val rddNew = sc.parallelize(1 to 100).map( v => StringHolder(Some(v.toString)))
    val dfNew = sqlContext.createDataFrame(rddNew)

    dfNew.write.format("h2o").mode(SaveMode.Overwrite).save("new_key")
    // load new H2O Frame
    val h2oFrame = DKV.getGet[Frame]("new_key")

    assert (dfNew.columns.length == h2oFrame.numCols(), "Number of columns should match")
    assert (dfNew.columns.sameElements(h2oFrame.names()),"Column names should match")
    assert (dfNew.count() == h2oFrame.numRows(), "Number of rows should match")
    h2oFrame.remove()
  }

  test("Writing to existing H2O Frame with ignore mode") {
    val rdd = sc.parallelize(1 to 1000).map( v => IntHolder(Some(v)))
    val df = sqlContext.createDataFrame(rdd)
    df.write.h2o("new_key")

    val rddNew = sc.parallelize(1 to 100).map( v => StringHolder(Some(v.toString)))
    val dfNew = sqlContext.createDataFrame(rddNew)
    dfNew.write.format("h2o").mode(SaveMode.Ignore).save("new_key")

    // load new H2O Frame
    val h2oFrame = DKV.getGet[Frame]("new_key")

    assert (df.columns.length == h2oFrame.numCols(), "Number of columns should match")
    assert (df.columns.sameElements(h2oFrame.names()),"Column names should match")
    assert (df.count() == h2oFrame.numRows(), "Number of rows should match")
    h2oFrame.remove()
  }

  test("Appending to existing H2O Frame ") {
    val rdd = sc.parallelize(1 to 1000).map( v => IntHolder(Some(v)))
    val df = sqlContext.createDataFrame(rdd)
    df.write.h2o("new_key")
    val rddNew = sc.parallelize(1 to 100).map( v => IntHolder(Some(v)))
    val dfNew = sqlContext.createDataFrame(rddNew)

    val thrown = intercept[RuntimeException] {
      dfNew.write.format("h2o").mode(SaveMode.Append).save("new_key")
    }
    assert(thrown.getMessage == "Appending to H2O Frame is not supported.")
  }
}
