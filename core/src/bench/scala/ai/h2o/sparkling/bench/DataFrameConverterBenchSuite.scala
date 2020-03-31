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

import ai.h2o.sparkling.ml.utils.{FlatArraysOnlySchema, FlatSchema, SchemaUtils, StructsOnlySchema}
import org.apache.spark.SparkContext
import org.apache.spark.h2o.testdata.{DenseVectorHolder, SparseVectorHolder}
import org.apache.spark.h2o.utils.{SharedH2OTestContext, TestFrameUtils}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vectors}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

import scala.util.Random

@RunWith(classOf[JUnitRunner])
class DataFrameConverterBenchSuite extends BenchSuite with SharedH2OTestContext {

  override def createSparkContext =
    new SparkContext("local-cluster[2, 1, 2048]", getClass.getSimpleName, defaultSparkConf)

  private val settings = TestFrameUtils.GenerateDataFrameSettings(
    numberOfRows = 8000,
    rowsPerPartition = 500,
    maxCollectionSize = 100,
    nullProbability = 0.0)

  benchTest("Measure performance of conversion to H2OFrame on a flat data frame") {
    testPerSchema(FlatSchema)
  }

  benchTest("Measure performance of conversion to H2OFrame on a data frame with nested structs") {
    testPerSchema(StructsOnlySchema)
  }

  benchTest("Measure performance of conversion to H2OFrame on a data frame with flat arrays") {
    testPerSchema(FlatArraysOnlySchema)
  }

  benchTest("Measure performance of flattening a data frame with nested structs") {
    testflattenOnlyPerSchema(StructsOnlySchema)
  }

  benchTest("Measure performance of flattening a data frame with flat arrays") {
    testflattenOnlyPerSchema(FlatArraysOnlySchema)
  }

  benchTest("Measure performance of flattening a schema with nested structs") {
    testflattenSchema(StructsOnlySchema)
  }

  benchTest("Measure performance of flattening a schema with flat arrays") {
    testflattenSchema(FlatArraysOnlySchema)
  }

  benchTest("Measure performance of row to schema with nested structs") {
    rowToSchema(StructsOnlySchema)
  }

  benchTest("Measure performance of row to schema with flat arrays") {
    rowToSchema(FlatArraysOnlySchema)
  }

  private def testPerSchema(schemaHolder: TestFrameUtils.SchemaHolder): Unit = {
    val df = TestFrameUtils.generateDataFrame(spark, schemaHolder, settings)
    val hf = hc.asH2OFrame(df)
    hf.remove()
  }

  private def testflattenOnlyPerSchema(schemaHolder: TestFrameUtils.SchemaHolder): Unit = {
    val df = TestFrameUtils.generateDataFrame(spark, schemaHolder, settings)
    SchemaUtils.flattenDataFrame(df).foreach(_ => {})
  }

  private def testflattenSchema(schemaHolder: TestFrameUtils.SchemaHolder): Unit = {
    val df = TestFrameUtils.generateDataFrame(spark, schemaHolder, settings)
    SchemaUtils.flattenSchema(df)
  }

  private def rowToSchema(schemaHolder: TestFrameUtils.SchemaHolder): Unit = {
    val df = TestFrameUtils.generateDataFrame(spark, schemaHolder, settings)
    SchemaUtils.rowsToRowSchemas(df).foreach(_ => {})
  }

  benchTest("Measure performance of conversion to H2OFrame on a data frame with wide sparse vectors") {
    import sqlContext.implicits._
    val numberOfCols = 50 * 1000
    val sparsity = 0.2
    val numberOfRows = 3 * 1000
    val partitions = 4
    val elementsPerRow = (sparsity * numberOfCols).toInt
    val rowGenerator = (_: Int) => SparseVectorHolder(sparseVector(numberOfCols, elementsPerRow))

    val df = sc.parallelize((0 until numberOfRows).map(row => rowGenerator(row)), partitions).toDF()

    val hf = hc.asH2OFrame(df)
    hf.remove()
  }

  benchTest("Measure performance of conversion to H2OFrame on a data frame with wide dense vectors") {
    import sqlContext.implicits._
    val numberOfCols = 10 * 1000
    val numberOfRows = 3 * 1000
    val partitions = 4

    val rowGenerator = (row: Int) => DenseVectorHolder(new DenseVector(Array.fill[Double](numberOfCols)(row)))

    val df = sc.parallelize((0 until numberOfRows).map(row => rowGenerator(row)), partitions).toDF()

    val hf = hc.asH2OFrame(df)
    hf.remove()
  }

  benchTest(
    "Measure performance of conversion to H2OFrame on a matrix 10x11 represented by sparse vectors",
    iterations = 10) {
    import sqlContext.implicits._

    val numberOfRows = 10
    val numberOfCols = 11
    val partitions = 2
    val rowGenerator = (row: Int) => {
      SparseVectorHolder(new SparseVector(numberOfCols, Array(row), Array[Double](row)))
    }
    val df = sc.parallelize((0 until numberOfRows).map(row => rowGenerator(row)), partitions).toDF()

    val hf = hc.asH2OFrame(df)
    hf.remove()
  }

  private def sparseVector(len: Int, elements: Int, rng: Random = Random): SparseVector = {
    assert(elements < len)
    val data = (1 to elements).map(_ => rng.nextInt(len)).sortBy(identity).distinct.map(it => (it, rng.nextDouble()))
    Vectors.sparse(len, data).toSparse
  }
}
