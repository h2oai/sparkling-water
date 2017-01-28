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
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.h2o.utils.SharedSparkTestContext
import org.apache.spark.sql.types.{StringType, IntegerType, StructField, StructType}
import org.apache.spark.sql.{Row, SaveMode}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.DKV

/**
  * Test using H2O Frame as Spark SQL data source
  */
@RunWith(classOf[JUnitRunner])
class DataSourceWithSchemaTestSuite extends FunSuite with SharedSparkTestContext {

  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-data-sources",
    conf = defaultSparkConf)

  test("Writing DataFrame to new H2O Frame ") {

    val schema = StructType(Seq(StructField("Column 1", IntegerType, nullable=true)))

    val rdd: RDD[Row] = sc.parallelize(1 to 1000).map( v => Row(v))
    
    val javaRDD: JavaRDD[Row] = rdd.toJavaRDD()
    
    val df = sqlContext.createDataFrame(javaRDD, schema)
    df.write.h2o("new_key")

    val h2oFrame = DKV.getGet[Frame]("new_key")
    assert (df.columns.length == h2oFrame.numCols(), "Number of columns should match")
    assert (df.columns.sameElements("Column 1"::Nil),"Column names should match")
    assert (df.count() == h2oFrame.numRows(), "Number of rows should match")
    h2oFrame.remove()
  }


  test("Writing DataFrame and then reading ") {

    val schema = StructType(Seq(StructField("Column 1", IntegerType, nullable=true)))

    val rdd: RDD[Row] = sc.parallelize(1 to 1000).map( v => Row(v))

    val javaRDD: JavaRDD[Row] = rdd.toJavaRDD()

    val df = sqlContext.createDataFrame(javaRDD, schema)
    df.write.h2o("new_key")

    
    val h2oFrame = DKV.getGet[Frame]("new_key")
    assert (df.columns.length == h2oFrame.numCols(), "Number of columns should match")
    assert (df.columns.sameElements("Column 1"::Nil),"Column names should match")
    assert (df.count() == h2oFrame.numRows(), "Number of rows should match")
    h2oFrame.remove()
  }

  test("Writing DataFrame to existing H2O Frame ") {
    val schema = StructType(Seq(StructField("Column 2", IntegerType, nullable=true)))

    val rdd: RDD[Row] = sc.parallelize(1 to 1000).map( v => Row(v))
    val javaRDD: JavaRDD[Row] = rdd.toJavaRDD()

    val df = sqlContext.createDataFrame(javaRDD, schema)
    df.write.h2o("new_key")

    val rddNew = sc.parallelize(1 to 1000).map( v => Row(v.toString))
    val javaRDDNew: JavaRDD[Row] = rddNew.toJavaRDD()
    val schema2 = StructType(Seq(StructField("Column 22", StringType, nullable=true)))
    val dfNew = sqlContext.createDataFrame(javaRDDNew, schema2)

    val h2oFrame = DKV.getGet[Frame]("new_key")
    val thrown = intercept[RuntimeException] {
      dfNew.write.format("h2o").mode(SaveMode.ErrorIfExists).save("new_key")
    }

    assert(thrown.getMessage == "Frame with key 'new_key' already exists, if you want to override it set the save mode to override.")
    h2oFrame.remove()
  }

  test("Overwriting existing H2O Frame ") {
    val schema = StructType(Seq(StructField("Column 3", IntegerType, nullable=true)))

    val rdd: RDD[Row] = sc.parallelize(1 to 1000).map( v => Row(v))
    val javaRDD: JavaRDD[Row] = rdd.toJavaRDD()
    val df = sqlContext.createDataFrame(javaRDD, schema)
    df.write.h2o("new_key")

    val rddNew = sc.parallelize(1 to 1000).map( v => Row(v.toString))
    val javaRDDNew: JavaRDD[Row] = rddNew.toJavaRDD()
    val schema2 = StructType(Seq(StructField("Column 33", StringType, nullable=true)))
    val dfNew = sqlContext.createDataFrame(javaRDDNew, schema2)

    dfNew.write.format("h2o").mode(SaveMode.Overwrite).save("new_key")
    // load new H2O Frame
    val h2oFrame = DKV.getGet[Frame]("new_key")

    assert (dfNew.columns.length == h2oFrame.numCols(), "Number of columns should match")
    assert (dfNew.columns.sameElements(h2oFrame.names()),"Column names should match")
    assert (dfNew.count() == h2oFrame.numRows(), "Number of rows should match")
    h2oFrame.remove()
  }

  test("Writing to existing H2O Frame with ignore mode") {
    val schema = StructType(Seq(StructField("Column 4", IntegerType, nullable=true)))

    val rdd: RDD[Row] = sc.parallelize(1 to 1000).map( v => Row(v))
    val javaRDD: JavaRDD[Row] = rdd.toJavaRDD()
    val df = sqlContext.createDataFrame(javaRDD, schema)
    df.write.h2o("new_key")

    val rddNew = sc.parallelize(1 to 1000).map( v => Row(v.toString))
    val javaRDDNew: JavaRDD[Row] = rddNew.toJavaRDD()
    val schema2 = StructType(Seq(StructField("Column 44", StringType, nullable=true)))
    val dfNew = sqlContext.createDataFrame(javaRDDNew, schema2)
    dfNew.write.format("h2o").mode(SaveMode.Ignore).save("new_key")

    // load new H2O Frame
    val h2oFrame = DKV.getGet[Frame]("new_key")

    assert (df.columns.length == h2oFrame.numCols(), "Number of columns should match")
    assert (df.columns.sameElements(h2oFrame.names()),"Column names should match")
    assert (df.count() == h2oFrame.numRows(), "Number of rows should match")
    h2oFrame.remove()
  }

  test("Appending to existing H2O Frame ") {
    val schema = StructType(Seq(StructField("Column 4", IntegerType, nullable=true)))

    val rdd: RDD[Row] = sc.parallelize(1 to 1000).map( v => Row(v))
    val javaRDD: JavaRDD[Row] = rdd.toJavaRDD()
    val df = sqlContext.createDataFrame(javaRDD, schema)
    df.write.h2o("new_key")

    val rddNew = sc.parallelize(1 to 100).map( v => Row(v))
    val dfNew = sqlContext.createDataFrame(rddNew, schema)

    val thrown = intercept[RuntimeException] {
      dfNew.write.format("h2o").mode(SaveMode.Append).save("new_key")
    }
    assert(thrown.getMessage == "Appending to H2O Frame is not supported.")
  }
}
