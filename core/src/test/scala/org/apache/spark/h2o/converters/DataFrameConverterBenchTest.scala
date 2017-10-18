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

package org.apache.spark.h2o.converters

import org.apache.spark.SparkContext
import org.apache.spark.h2o.testdata.SparseVectorHolder
import org.apache.spark.h2o.utils.SharedH2OTestContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.api.TestUtils
import water.util.PrettyPrint

@RunWith(classOf[JUnitRunner])
class DataFrameConverterBenchTest extends FunSuite with SharedH2OTestContext {

  override def createSparkContext: SparkContext = new SparkContext("local[*]", "bench-local", conf = defaultSparkConf)

  // @formatter:off
  test("Wide dataset to H2OFrame") {
    import org.apache.spark.h2o.utils.BenchUtils.bench
    import TestUtils.sparseVector
    import sqlContext.implicits._
    val NCOL = 50*1000
    val SPARSITY = 0.2
    val NROW = 3*1000
    val PARTITIONS = 1

    val elementsPerRow = (SPARSITY * NCOL).toInt
    val rowGenerator = (row: Int) => {
      new SparseVectorHolder(sparseVector(NCOL, elementsPerRow))
    }
    val df = sc.parallelize((0 until NROW).map(row => rowGenerator(row)), PARTITIONS).toDF()

    val m1 = bench(5) {
      val hf = hc.asH2OFrame(df)
      hf.remove()
    }

    println(m1.show())
  }

  test("Convert matrix 10x11 to H2OFrame") {
    import org.apache.spark.h2o.utils.BenchUtils.bench
    import sqlContext.implicits._

    val NROW = 10
    val NCOL = 11
    val PARTITIONS = 1
    val rowGenerator = (row: Int) => {
      new SparseVectorHolder(new org.apache.spark.ml.linalg.SparseVector(NCOL, Array(row), Array[Double](row)))
    }
    val df = sc.parallelize((0 until NROW).map(row => rowGenerator(row)), PARTITIONS).toDF()

    val m1 = bench(10) {
      val hf = hc.asH2OFrame(df)
      hf.remove()
    }

    println(m1.show())
  }
}
