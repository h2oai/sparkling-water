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

package ai.h2o.sparkling.backend.converters

import ai.h2o.sparkling.SharedH2OTestContext
import org.apache.spark.ExposeUtils
import org.apache.spark.ml.linalg.SQLDataTypes.VectorType
import org.apache.spark.ml.linalg._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.scalatest.{FunSuite, Matchers}

class DataFrameConversionSuiteBase extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  protected case class ColumnSpecification[T](field: StructField, valueGenerator: Int => Seq[Any])

  protected def generateBooleans(numberOfRows: Int): Seq[Boolean] = generateIntegers(numberOfRows).map(i => i % 2 == 0)

  protected def generateBytes(numberOfRows: Int): Seq[Byte] = generateIntegers(numberOfRows).map(_.byteValue())

  protected def generateShorts(numberOfRows: Int): Seq[Short] = generateIntegers(numberOfRows).map(_.shortValue())

  protected def generateIntegers(numberOfRows: Int): Seq[Integer] = (1 to numberOfRows).map(new Integer(_))

  protected def generateLongs(numberOfRows: Int): Seq[Long] = generateIntegers(numberOfRows).map(_.longValue())

  protected def generateTimestamps(numberOfRows: Int): Seq[java.sql.Timestamp] = {
    generateLongs(numberOfRows).map(new java.sql.Timestamp(_))
  }

  protected def generateDates(numberOfRows: Int): Seq[java.sql.Date] = {
    generateLongs(numberOfRows).map(new java.sql.Date(_))
  }

  protected def generateFloats(numberOfRows: Int): Seq[Float] = {
    generateIntegers(numberOfRows).map(i => i.floatValue() + i.floatValue() / 100)
  }

  protected def generateDoubles(numberOfRows: Int): Seq[Double] = {
    generateIntegers(numberOfRows).map(i => i.doubleValue() + i.doubleValue() / 100)
  }

  protected def generateStrings(numberOfRows: Int): Seq[String] = generateIntegers(numberOfRows).map(_.toString())

  protected def generateDenseVectors(numberOfRows: Int): Seq[Vector] = {
    (1 to numberOfRows).map { size =>
      val doubles = generateDoubles(size).zipWithIndex.map { case (v, i) => if (i % 2 == 0) 0.0 else v }.toArray
      Vectors.dense(doubles)
    }
  }

  protected def generateSparseVectors(numberOfRows: Int): Seq[Vector] = {
    def filterCondition(i: Int): Boolean = i % 2 == 0

    (1 to numberOfRows).map { size =>
      val rawDoubles = generateDoubles(size)
      val doubles = generateDoubles(size).zipWithIndex.withFilter(pair => filterCondition(pair._2)).map(_._1).toArray
      val indices = rawDoubles.indices.filter(filterCondition).toArray
      Vectors.sparse(size, indices, doubles)
    }
  }

  protected val simpleColumns = Seq(
    ColumnSpecification(StructField("Booleans", BooleanType, nullable = false), generateBooleans),
    ColumnSpecification(StructField("Bytes", ByteType, nullable = false), generateBytes),
    ColumnSpecification(StructField("Shorts", ShortType, nullable = false), generateShorts),
    ColumnSpecification(StructField("Integers", IntegerType, nullable = false), generateIntegers),
    ColumnSpecification(StructField("Longs", LongType, nullable = false), generateLongs),
    ColumnSpecification(StructField("Floats", FloatType, nullable = false), generateFloats),
    ColumnSpecification(StructField("Doubles", DoubleType, nullable = false), generateDoubles),
    ColumnSpecification(StructField("Strings", StringType, nullable = false), generateStrings),
    ColumnSpecification(StructField("Timestamps", TimestampType, nullable = false), generateTimestamps),
    ColumnSpecification(StructField("Dates", DateType, nullable = false), generateDates))

  protected val vectorColumns = Seq(
    ColumnSpecification(StructField("SparseVectors", VectorType, nullable = false), generateSparseVectors),
    ColumnSpecification(StructField("DenseVectors", VectorType, nullable = false), generateDenseVectors))

  protected val simpleColumnsWithNulls = simpleColumns.map {
    case ColumnSpecification(field, valueGenerator) =>
      ColumnSpecification(
        StructField(field.name + "AndSomeNulls", field.dataType, nullable = true),
        valueGenerator.andThen { values: Seq[Any] =>
          values.zipWithIndex.map { case (value, index) => if (index % 4 == 0) null else value }
        })
  }

  protected val allColumns = simpleColumns ++ simpleColumnsWithNulls ++ vectorColumns // Nullable vectors are not supported

}
