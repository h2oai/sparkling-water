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

import org.apache.spark.{ExposeUtils, SparkContext}
import org.apache.spark.h2o.utils.{SharedH2OTestContext, TestFrameUtils}
import org.apache.spark.ml.linalg._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row}
import org.scalatest.{FunSuite, Matchers}

class DataExchangeTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkContext: SparkContext = new SparkContext("local[*]", getClass.getSimpleName, conf = defaultSparkConf)

  case class ColumnSpecification[T](field: StructField, valueGenerator: (Int) => Seq[Any])

  def generateBooleans(numberOfRows: Int): Seq[Boolean] = generateIntegers(numberOfRows).map(i => i % 2 == 0)

  def generateBytes(numberOfRows: Int): Seq[Byte] = generateIntegers(numberOfRows).map(_.byteValue())

  def generateShorts(numberOfRows: Int): Seq[Short] = generateIntegers(numberOfRows).map(_.shortValue())

  def generateIntegers(numberOfRows: Int): Seq[Integer] = (1 to numberOfRows).map(new Integer(_))

  def generateLongs(numberOfRows: Int): Seq[Long] = generateIntegers(numberOfRows).map(_.longValue())

  def generateTimestamps(numberOfRows: Int): Seq[java.sql.Timestamp] = {
    generateLongs(numberOfRows).map(new java.sql.Timestamp(_))
  }

  def generateDates(numberOfRows: Int): Seq[java.sql.Date] = {
    generateLongs(numberOfRows).map(new java.sql.Date(_))
  }

  def generateFloats(numberOfRows: Int): Seq[Float] = {
    generateIntegers(numberOfRows).map(i => i.floatValue() + i.floatValue() / 100)
  }

  def generateDoubles(numberOfRows: Int): Seq[Double] = {
    generateIntegers(numberOfRows).map(i => i.doubleValue() + i.doubleValue() / 100)
  }

  def generateStrings(numberOfRows: Int): Seq[String] = generateIntegers(numberOfRows).map(_.toString())

  def generateDenseVectors(numberOfRows: Int): Seq[Vector] = {
    (1 to numberOfRows).map { size =>
      val doubles = generateDoubles(size).zipWithIndex.map{ case (v, i) => if (i % 2 == 0) 0.0 else v }.toArray
      Vectors.dense(doubles)
    }
  }

  def generateSparseVectors(numberOfRows: Int): Seq[Vector] = {
    def filterCondition(i: Int): Boolean = i % 2 == 0
    (1 to numberOfRows).map { size =>
      val rawDoubles = generateDoubles(size)
      val doubles = generateDoubles(size).zipWithIndex.withFilter(pair => filterCondition(pair._2)).map(_._1).toArray
      val indices = rawDoubles.indices.filter(filterCondition).toArray
      Vectors.sparse(size, indices, doubles)
    }
  }

  val simpleColumns = Seq(
    ColumnSpecification(StructField("Booleans", BooleanType, false), generateBooleans),
    ColumnSpecification(StructField("Bytes", ByteType, false), generateBytes),
    ColumnSpecification(StructField("Shorts", ShortType, false), generateShorts),
    ColumnSpecification(StructField("Integers", IntegerType, false), generateIntegers),
    ColumnSpecification(StructField("Longs", LongType, false), generateLongs),
    ColumnSpecification(StructField("Floats", FloatType, false), generateFloats),
    ColumnSpecification(StructField("Doubles", DoubleType, false), generateDoubles),
    ColumnSpecification(StructField("Strings", StringType, false), generateStrings),
    ColumnSpecification(StructField("Timestamps", TimestampType, false), generateTimestamps),
    ColumnSpecification(StructField("Dates", DateType, false), generateDates))

  val vectorColumns = Seq(
    ColumnSpecification(StructField("SparseVectors", new VectorUDT, false), generateSparseVectors),
    ColumnSpecification(StructField("DenseVectors", new VectorUDT, false), generateDenseVectors))


  val simpleColumnsWithNulls = simpleColumns.map { case ColumnSpecification(field, valueGenerator) =>
    ColumnSpecification(
      StructField(field.name + "AndSomeNulls", field.dataType, true),
      valueGenerator.andThen { (values: Seq[Any]) =>
        values.zipWithIndex.map{ case (value, index) => if (index % 4 == 0) null else value }
      })
  }

  val allColumns = simpleColumns ++ simpleColumnsWithNulls ++ vectorColumns // Nullable vectors are not supported

  def testConversionFromSparkToH2OAndBack(first: ColumnSpecification[_], second: ColumnSpecification[_]): Unit = {
    test(s"Convert DataFrame of [${first.field.name}, ${second.field.name}] to H2OFrame and back") {
      val numberOfRows = 20
      val firstValues = first.valueGenerator(numberOfRows)
      val secondValues = second.valueGenerator(numberOfRows)
      val values = firstValues.zip(secondValues).map { case (f, s) => Row(f, s) }
      val rdd = spark.sparkContext.parallelize(values, 4)
      val dataFrame = spark.createDataFrame(rdd, StructType(first.field :: second.field :: Nil))

      val expectedDataFrame = getExpectedDataFrame(dataFrame, numberOfRows)
      val h2oFrame = hc.asH2OFrame(dataFrame)
      val result = hc.asDataFrame(h2oFrame)

      TestFrameUtils.assertDataFramesAreIdentical(expectedDataFrame, result)
    }
  }

  def getExpectedDataFrame(original: DataFrame, vectorSize: Int): DataFrame = {
    val newColumns = original.schema.fields.flatMap { case StructField(name, dataType, _, _) =>
      val column = col(name)
      dataType match {
        case BooleanType => Seq(column.cast(ByteType))
        case v if ExposeUtils.isMLVectorUDT(v) =>
          val toArr: Any => Array[Double] = (input: Any) => {
            val values = input.asInstanceOf[org.apache.spark.ml.linalg.Vector].toArray
            values ++ Array.fill(vectorSize - values.length)(0.0)
          }
          val toArrUdf = udf(toArr)
          val arrayColumn = toArrUdf(column)
          (0 until vectorSize).map(i => arrayColumn.getItem(i).as(name + i))
        case _ => Seq(column)
      }
    }
    original.select(newColumns: _*)
  }

  allColumns.combinations(2).foreach { case Seq(first, second) =>
    testConversionFromSparkToH2OAndBack(first, second)
    testConversionFromSparkToH2OAndBack(second, first)
  }
}
