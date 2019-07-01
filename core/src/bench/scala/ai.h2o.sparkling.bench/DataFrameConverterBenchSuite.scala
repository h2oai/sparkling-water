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

package ai.h2o.sparkling.bench

import ai.h2o.sparkling.utils.schemas._
import org.apache.spark.SparkContext
import org.apache.spark.h2o.testdata.{DenseVectorHolder, SparseVectorHolder}

import org.apache.spark.h2o.utils.{SharedH2OTestContext, TestFrameUtils}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import water.api.TestUtils

@RunWith(classOf[JUnitRunner])
class DataFrameConverterBenchSuite extends BenchSuite with SharedH2OTestContext {
  val conf = defaultSparkConf

  override def createSparkContext = new SparkContext("local-cluster[2, 1, 2048]", this.getClass.getSimpleName, conf)

  val settings = TestFrameUtils.GenerateDataFrameSettings(
    numberOfRows = 8000,
    rowsPerPartition = 500,
    maxCollectionSize = 100,
    nullProbability = 0.0
  )

  benchTest("Measure performance of conversion to H2OFrame on a flat data frame") {
    testPerSchema(FlatSchema)
  }

  benchTest("Measure performance of conversion to H2OFrame on a data frame with nested structs") {
    testPerSchema(StructsOnlySchema)
  }

  benchTest("Measure performance of conversion to H2OFrame on a data frame with flat arrays") {
    testPerSchema(FlatArraysOnlySchema)
  }

  def testPerSchema(schemaHolder: TestFrameUtils.SchemaHolder): Unit = {
    val df = TestFrameUtils.generateDataFrame(spark, schemaHolder, settings)
    val hf = hc.asH2OFrame(df)
    hf.remove()
  }

  benchTest("Measure performance of conversion to H2OFrame on a data frame with wide sparse vectors") {
    import TestUtils.sparseVector
    import sqlContext.implicits._
    val numberOfCols = 50 * 1000
    val sparsity = 0.2
    val numberOfRows = 3 * 1000
    val partitions = 4

    val elementsPerRow = (sparsity * numberOfCols).toInt
    val rowGenerator = (row: Int) => new SparseVectorHolder(sparseVector(numberOfCols, elementsPerRow))

    val df = sc.parallelize((0 until numberOfRows).map(row => rowGenerator(row)), partitions).toDF()

    val hf = hc.asH2OFrame(df)
    hf.remove()
  }

  benchTest("Measure performance of conversion to H2OFrame on a data frame with wide dense vectors") {
    import sqlContext.implicits._
    val numberOfCols = 10 * 1000
    val numberOfRows = 3 * 1000
    val partitions = 4

    val rowGenerator = (row: Int) => new DenseVectorHolder(new DenseVector(Array.fill[Double](numberOfCols)(row)))

    val df = sc.parallelize((0 until numberOfRows).map(row => rowGenerator(row)), partitions).toDF()

    val hf = hc.asH2OFrame(df)
    hf.remove()
  }

  benchTest("Measure performance of conversion to H2OFrame on a matrix 10x11 represented by sparse vectors", iterations = 10) {
    import sqlContext.implicits._

    val numberOfRows = 10
    val numberOfCols = 11
    val partitions = 2
    val rowGenerator = (row: Int) => {
      new SparseVectorHolder(new SparseVector(numberOfCols, Array(row), Array[Double](row)))
    }
    val df = sc.parallelize((0 until numberOfRows).map(row => rowGenerator(row)), partitions).toDF()

    val hf = hc.asH2OFrame(df)
    hf.remove()
  }
}
