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
import org.apache.spark.h2o.util.SharedSparkTestContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.fvec.Vec
import water.parser.BufferedString

/**
  * Testing schema for h2o schema spark dataset transformation.
  */

case class SamplePerson(name: String, age: Int, email: String)

@RunWith(classOf[JUnitRunner])
class H2ODatasetTest  extends FunSuite with SharedSparkTestContext {
  val people:List[SamplePerson] =
    SamplePerson("Hermione Granger", 15, "hgranger@griffindor.edu.uk")::
    SamplePerson("Ron Weasley", 14, "rweasley@griffindor.edu.uk")::
    SamplePerson("Harry Potter", 14, "hpotter@griffindor.edu.uk")::
    SamplePerson("Lucius Malfoy", 13, "lucius@slitherin.edu.uk")::Nil
  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)

  test("Dataset[SamplePerson] to H2OFrame and back") {
    val sqlContext = hsc.sqlContext
//    val rdd:RDD[Product] = sqlContext.sparkContext.parallelize(people)
//    val df = rdd.toDF()
//    df.registerTempTable("TestDataSamplePerson")

    import sqlContext.implicits._

    val ds = sqlContext.createDataset(people)
    val h2oFrame:H2OFrame = hsc.asH2OFrame(ds)

    assertBasicInvariants(ds, h2oFrame, (row, vec) => {
      val sample = people(row.toInt)
      val valueString = new BufferedString()

      val value = vec.atStr(valueString, row) // value stored at row-th
      assert (sample.name == value.toString, s"The H2OFrame values should match")
    }, List("name", "age", "email"))
//    val backToDs = hsc.asDataset

    val asrdd = hc.asRDD[SamplePerson](h2oFrame)
    val asDS = asrdd.toDS()
    val extracted = asDS.collect() // this one crashes
    assert(ds.count == h2oFrame.numRows(), "Number of rows should match")
//    assert(asrdd.count == h2oFrame.numRows(), "Number of rows should match")
    // Clean up
    h2oFrame.delete()
    ds.unpersist()
  }

  private type RowValueAssert = (Long, Vec) => Unit

  private def assertBasicInvariants[T<:Product](ds: Dataset[T], df: H2OFrame, rowAssert: RowValueAssert, names: List[String]): Unit = {
    assertHolderProperties(df, names)
    assert (ds.count == df.numRows(), "Number of rows in H2OFrame and Dataset should match")
    // Check numbering
    val vec = df.vec(0)
    var row = 0
    while(row < df.numRows()) {
      assert (!vec.isNA(row), "The H2OFrame should not contain any NA values")
      rowAssert (row, vec)
      row += 1
    }
  }

  private def assertHolderProperties(df: H2OFrame, names: List[String]): Unit = {
    val actualNames = df.names().toList
    val numCols = names.length
    assert (df.numCols() == numCols, s"H2OFrame should contain $numCols column(s), have ${df.numCols()}")
    assert (df.names().length == numCols, s"H2OFrame column names should be $numCols in size, have ${df.names().length}")
    assert (actualNames.equals(names),
      s"H2OFrame column names should be $names since Holder object was used to define Dataset, but it is $actualNames")
  }
}
