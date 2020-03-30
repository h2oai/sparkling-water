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

package org.apache.spark.h2o.utils

import java.util.UUID

import org.apache.spark.h2o.{Dataset, H2OFrame}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{lit, rand}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.scalatest.Matchers
import water.fvec._
import water.parser.BufferedString

import scala.reflect.ClassTag
import scala.util.Random

/**
  * Various helpers to help with working with Frames during tests
  */
object TestFrameUtils extends Matchers {
  def makeH2OFrame[T: ClassTag](
      fname: String,
      colNames: Array[String],
      chunkLayout: Array[Long],
      data: Array[Array[T]],
      h2oType: Byte,
      colDomains: Array[Array[String]] = null): H2OFrame = {
    makeH2OFrame2(fname, colNames, chunkLayout, data.map(_.map(value => Array(value))), Array(h2oType), colDomains)
  }

  def makeH2OFrame2[T: ClassTag](
      fname: String,
      colNames: Array[String],
      chunkLayout: Array[Long],
      data: Array[Array[Array[T]]],
      h2oTypes: Array[Byte],
      colDomains: Array[Array[String]] = null): H2OFrame = {
    ChunkUtils.initFrame(fname, colNames)

    for (i <- chunkLayout.indices) {
      buildChunks(fname, data(i), i, h2oTypes)
    }

    new H2OFrame(ChunkUtils.finalizeFrame(fname, chunkLayout, h2oTypes, colDomains))
  }

  def buildChunks[T: ClassTag](
      fname: String,
      data: Array[Array[T]],
      cidx: Integer,
      h2oType: Array[Byte]): Array[_ <: Chunk] = {
    val nchunks: Array[NewChunk] = ChunkUtils.createNewChunks(fname, h2oType, cidx)

    data.foreach { values =>
      values.indices.foreach { idx =>
        val chunk: NewChunk = nchunks(idx)
        values(idx) match {
          case null => chunk.addNA()
          case u: UUID => chunk.addUUID(u.getLeastSignificantBits, u.getMostSignificantBits)
          case s: String => chunk.addStr(new BufferedString(s))
          case b: Byte => chunk.addNum(b)
          case s: Short => chunk.addNum(s)
          case c: Integer if h2oType(0) == Vec.T_CAT => chunk.addCategorical(c)
          case i: Integer if h2oType(0) != Vec.T_CAT => chunk.addNum(i.toDouble)
          case l: Long => chunk.addNum(l)
          case d: Double => chunk.addNum(d)
          case x =>
            throw new IllegalArgumentException(s"Failed to figure out what is it: $x")
        }
      }
    }
    ChunkUtils.closeNewChunks(nchunks)
    nchunks
  }

  def assertFieldNamesAreEqual(expected: DataFrame, produced: DataFrame): Unit = {
    def fieldNames(df: DataFrame) = df.schema.fields.map(_.name)

    val expectedNames = fieldNames(expected)
    val producedNames = fieldNames(produced)
    producedNames shouldEqual expectedNames
  }

  def assertDataFramesAreIdentical(expected: DataFrame, produced: DataFrame): Unit = {
    expected.cache()
    produced.cache()
    val expectedCount = expected.count()
    val producedCount = produced.count()
    assert(
      expectedCount == producedCount,
      s"""The expected data frame has $expectedCount rows whereas
         |the produced data frame has $producedCount rows.""".stripMargin)

    val expectedDistinctCount = expected.distinct().count()
    val producedDistinctCount = produced.distinct().count()
    assert(
      expectedDistinctCount == producedDistinctCount,
      s"""The expected data frame has $expectedDistinctCount distinct rows whereas
         |the produced data frame has $producedDistinctCount distinct rows.""".stripMargin)

    val numberOfExtraRowsInExpected = expected.except(produced).count()
    val numberOfExtraRowsInProduced = produced.except(expected).count()
    assert(
      numberOfExtraRowsInExpected == 0 && numberOfExtraRowsInProduced == 0,
      s"""The expected data frame contains $numberOfExtraRowsInExpected distinct rows that are not in the produced data frame.
         |The produced data frame contains $numberOfExtraRowsInProduced distinct rows that are not in the expected data frame.
       """.stripMargin)
  }

  private type RowValueAssert = (Long, Vec) => Unit

  def assertBasicInvariants[T <: Product](rdd: RDD[T], df: H2OFrame, rowAssert: RowValueAssert): Unit = {
    assertRDDHolderProperties(df)
    assert(rdd.count == df.numRows(), "Number of rows in H2OFrame and RDD should match")
    // Check numbering
    val vec = df.vec(0)
    var rowIdx = 0
    while (rowIdx < df.numRows()) {
      assert(!vec.isNA(rowIdx), "The H2OFrame should not contain any NA values")
      rowAssert(rowIdx, vec)
      rowIdx += 1
    }
  }

  def assertInvariantsWithNulls[T <: Product](rdd: RDD[T], df: H2OFrame, rowAssert: RowValueAssert): Unit = {
    assertRDDHolderProperties(df)
    assert(rdd.count == df.numRows(), "Number of rows in H2OFrame and RDD should match")
    // Check numbering
    val vec = df.vec(0)
    var rowIdx = 0
    while (rowIdx < df.numRows()) {
      rowAssert(rowIdx, vec)
      rowIdx += 1
    }
  }

  private def assertRDDHolderProperties(df: H2OFrame): Unit = {
    assert(df.numCols() == 1, "H2OFrame should contain single column")
    assert(df.names().length == 1, "H2OFrame column names should have single value")
    assert(
      df.names()(0).equals("result"),
      "H2OFrame column name should be 'result' since Holder object was used to define RDD")
  }

  private def assertDatasetHolderProperties(df: H2OFrame, names: List[String]): Unit = {
    val actualNames = df.names().toList
    val numCols = names.length
    assert(df.numCols() == numCols, s"H2OFrame should contain $numCols column(s), have ${df.numCols()}")
    assert(df.names().length == numCols, s"H2OFrame column names should be $numCols in size, have ${df.names().length}")
    assert(
      actualNames.equals(names),
      s"H2OFrame column names should be $names since Holder object was used to define Dataset, but it is $actualNames")
  }

  def assertBasicInvariants[T <: Product](
      ds: Dataset[T],
      df: H2OFrame,
      rowAssert: RowValueAssert,
      names: List[String]): Unit = {
    assertDatasetHolderProperties(df, names)
    assert(
      ds.count == df.numRows(),
      s"Number of rows in H2OFrame (${df.numRows()}) and Dataset (${ds.count}) should match")

    val vec = df.vec(0)
    for (row <- Range(0, df.numRows().toInt)) {
      rowAssert(row, vec)
    }
  }

  case class GenerateDataFrameSettings(
      numberOfRows: Int,
      rowsPerPartition: Int,
      maxCollectionSize: Int,
      nullProbability: Double = 0.1,
      seed: Long = 1234L)

  trait SchemaHolder {
    def schema: StructType
  }

  def generateDataFrame(
      spark: SparkSession,
      schemaHolder: SchemaHolder,
      settings: GenerateDataFrameSettings): DataFrame = {
    implicit val encoder = RowEncoder(schemaHolder.schema)
    val numberOfPartitions = Math.max(1, settings.numberOfRows / settings.rowsPerPartition)
    spark
      .range(settings.numberOfRows)
      .repartition(numberOfPartitions)
      .select(rand(settings.seed) * lit(Long.MaxValue) cast LongType)
      .map { r: Row =>
        val schema = schemaHolder.schema
        val localRandom = new Random(r.getLong(0))
        val values = schema.fields.map(f => generateValueForField(localRandom, f, settings))
        new GenericRowWithSchema(values, schema)
      }
  }

  private def generateValueForField(
      random: Random,
      field: StructField,
      settings: GenerateDataFrameSettings,
      prefix: Option[String] = None): Any = {
    val StructField(name, dataType, nullable, _) = field
    val nameWithPrefix = prefix match {
      case None => name
      case Some(x) => s"${x}_name"
    }
    if (nullable && random.nextDouble() < settings.nullProbability) {
      null
    } else {
      dataType match {
        case BooleanType => random.nextBoolean()
        case ByteType => random.nextInt(255).toByte
        case ShortType => random.nextInt(256 * 256 - 1).toShort
        case IntegerType => random.nextInt()
        case LongType => random.nextLong()
        case DoubleType => random.nextDouble()
        case d: DecimalType => BigDecimal(1L, d.scale)
        case StringType => s"${name}_${random.nextInt()}"
        case ArrayType(elementType, containsNull) =>
          generateArray(random, settings, elementType, containsNull, nameWithPrefix)
        case BinaryType =>
          generateArray(random, settings, ByteType, false, nameWithPrefix)
        case MapType(keyType, valueType, valueContainsNull) =>
          val array = generateArray(random, settings, valueType, valueContainsNull, nameWithPrefix)
          array.zipWithIndex.map {
            case (a, i) =>
              val keyField = StructField(i.toString, keyType, valueContainsNull)
              val key = generateValueForField(random, keyField, settings, Some(nameWithPrefix))
              key -> a
          }.toMap
        case struct @ StructType(fields) =>
          val values = fields.map(f => generateValueForField(random, f, settings, Some(nameWithPrefix)))
          new GenericRowWithSchema(values, struct)
      }
    }
  }

  private def generateArray(
      random: Random,
      settings: GenerateDataFrameSettings,
      elementType: DataType,
      containsNull: Boolean,
      nameWithPrefix: String): Seq[Any] = {
    (0 until random.nextInt(settings.maxCollectionSize)).map { idx =>
      val arrayField = StructField(idx.toString, elementType, containsNull)
      generateValueForField(random, arrayField, settings, Some(nameWithPrefix))
    }
  }
}
