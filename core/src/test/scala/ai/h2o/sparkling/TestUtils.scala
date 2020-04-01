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
package ai.h2o.sparkling

import java.io.File
import java.sql.Timestamp
import java.util.UUID

import org.apache.spark.h2o.Dataset
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.functions.{lit, rand}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.{h2o, mllib}
import org.scalatest.Matchers
import water.fvec.{Chunk, ChunkUtils, NewChunk, Vec}
import water.parser.BufferedString

import scala.reflect.ClassTag
import scala.util.Random

object TestUtils extends Matchers {

  def locate(name: String): String = {
    val file = new File("./examples/" + name)
    if (file.exists()) {
      file.getAbsolutePath
    } else {
      // testing from IDEA
      new File("../examples/" + name).getAbsolutePath
    }
  }

  def assertVectorIntValues(vec: water.fvec.Vec, values: Seq[Int]): Unit = {
    (0 until vec.length().toInt).foreach { rIdx =>
      assert(
        if (vec.isNA(rIdx)) -1 == values(rIdx)
        else vec.at8(rIdx) == values(rIdx),
        "Values stored in H2OFrame has to match specified values")
    }
  }

  def assertVectorDoubleValues(vec: water.fvec.Vec, values: Seq[Double]): Unit = {
    (0 until vec.length().toInt).foreach { rIdx =>
      assert(
        if (vec.isNA(rIdx)) values(rIdx).equals(Double.NaN) // this is Scala i can do NaN comparision
        else vec.at(rIdx) == values(rIdx),
        "Values stored in H2OFrame has to match specified values")
    }
  }

  def assertVectorStringValues(vec: water.fvec.Vec, values: Seq[String]): Unit = {
    val valString = new BufferedString()
    (0 until vec.length().toInt).foreach { rIdx =>
      assert(vec.isNA(rIdx) || {
        vec.atStr(valString, rIdx)
        valString.toSanitizedString == values(rIdx)
      }, "Values stored in H2OFrame has to match specified values")
    }
  }

  def assertDoubleFrameValues(f: water.fvec.Frame, rows: Seq[Array[Double]]): Unit = {
    val ncol = f.numCols()
    val rowsIdx = (0 until f.numRows().toInt)
    val columns = (0 until ncol).map(cidx => rowsIdx.map(rows(_)(cidx)))
    f.vecs().zipWithIndex.foreach {
      case (vec, idx: Int) =>
        assertVectorDoubleValues(vec, columns(idx))
    }
  }

  def makeH2OFrame[T: ClassTag](
      fname: String,
      colNames: Array[String],
      chunkLayout: Array[Long],
      data: Array[Array[T]],
      h2oType: Byte,
      colDomains: Array[Array[String]] = null): h2o.H2OFrame = {
    makeH2OFrame2(fname, colNames, chunkLayout, data.map(_.map(value => Array(value))), Array(h2oType), colDomains)
  }

  def makeH2OFrame2[T: ClassTag](
      fname: String,
      colNames: Array[String],
      chunkLayout: Array[Long],
      data: Array[Array[Array[T]]],
      h2oTypes: Array[Byte],
      colDomains: Array[Array[String]] = null): h2o.H2OFrame = {
    ChunkUtils.initFrame(fname, colNames)

    for (i <- chunkLayout.indices) {
      buildChunks(fname, data(i), i, h2oTypes)
    }

    new h2o.H2OFrame(ChunkUtils.finalizeFrame(fname, chunkLayout, h2oTypes, colDomains))
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

  def assertBasicInvariants[T <: Product](rdd: RDD[T], df: h2o.H2OFrame, rowAssert: RowValueAssert): Unit = {
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

  def assertInvariantsWithNulls[T <: Product](rdd: RDD[T], df: h2o.H2OFrame, rowAssert: RowValueAssert): Unit = {
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

  private def assertRDDHolderProperties(df: h2o.H2OFrame): Unit = {
    assert(df.numCols() == 1, "H2OFrame should contain single column")
    assert(df.names().length == 1, "H2OFrame column names should have single value")
    assert(
      df.names()(0).equals("value"),
      "H2OFrame column name should be 'value' since we define the value inside the Option.")
  }

  private def assertDatasetHolderProperties(df: h2o.H2OFrame, names: List[String]): Unit = {
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
      df: h2o.H2OFrame,
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

  case class ByteField(v: Byte)

  case class ShortField(v: Short)

  case class IntField(v: Int)

  case class LongField(v: Long)

  case class FloatField(v: Float)

  case class DoubleField(v: Double)

  case class StringField(v: String)

  case class TimestampField(v: Timestamp)

  case class DateField(d: java.sql.Date)

  case class PrimitiveA(n: Int, name: String)

  case class ComposedA(a: PrimitiveA, weight: Double)

  case class ComposedWithTimestamp(a: PrimitiveA, v: TimestampField)

  case class PrimitiveB(f: Seq[Int])

  case class PrimitiveMllibFixture(f: mllib.linalg.Vector)

  case class PrimitiveMlFixture(f: org.apache.spark.ml.linalg.Vector)

  case class ComplexMlFixture(f1: org.apache.spark.ml.linalg.Vector, idx: Int, f2: org.apache.spark.ml.linalg.Vector)

  case class Prostate(
      ID: Option[Long],
      CAPSULE: Option[Int],
      AGE: Option[Int],
      RACE: Option[Int],
      DPROS: Option[Int],
      DCAPS: Option[Int],
      PSA: Option[Float],
      VOL: Option[Float],
      GLEASON: Option[Int]) {
    def isWrongRow: Boolean = (0 until productArity).map(idx => productElement(idx)).forall(e => e == None)
  }

  class PUBDEV458Type(val result: Option[Int]) extends Product with Serializable {
    override def canEqual(that: Any): Boolean = that.isInstanceOf[PUBDEV458Type]

    override def productArity: Int = 1

    override def productElement(n: Int) =
      n match {
        case 0 => result
        case _ => throw new IndexOutOfBoundsException(n.toString)
      }
  }

  case class OptionAndNot(val x: Option[Int], val y: Option[Int]) extends Serializable

  case class SamplePerson(name: String, age: Int, email: String)

  case class WeirdPerson(email: String, age: Int, name: String)

  case class SampleCompany(officialName: String, count: Int, url: String)

  case class SampleAccount(email: String, name: String, age: Int)

  case class SampleCat(name: String, age: Int)

  case class PartialPerson(name: Option[String], age: Option[Int], email: Option[String])

  case class SemiPartialPerson(name: String, age: Option[Int], email: Option[String])

  case class SampleString(x: String)

  case class SampleAltString(y: String)

  case class SparseVectorHolder(v: org.apache.spark.ml.linalg.SparseVector)

  case class DenseVectorHolder(v: org.apache.spark.ml.linalg.DenseVector)

  case class Name(given: String, family: String)

  case class Person(name: Name, age: Int)

  case class StringHolder(result: String)

  case class DoubleHolder(result: Double)
}
