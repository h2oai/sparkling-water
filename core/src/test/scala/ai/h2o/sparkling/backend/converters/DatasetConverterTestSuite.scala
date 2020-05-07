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

import ai.h2o.sparkling.TestUtils._
import ai.h2o.sparkling.{H2OFrame, SharedH2OTestContext}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

@RunWith(classOf[JUnitRunner])
class DatasetConverterTestSuite extends FunSuite with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")
  import spark.implicits._

  private val dataSource =
    ("Hermione Granger", 15, "hgranger@griffindor.edu.uk") ::
      ("Ron Weasley", 14, "rweasley@griffindor.edu.uk") ::
      ("Harry Potter", 14, "hpotter@griffindor.edu.uk") ::
      ("Lucius Malfoy", 13, "lucius@slitherin.edu.uk") :: Nil

  private val samplePeople: List[SamplePerson] = dataSource map SamplePerson.tupled

  private val samplePartialPeople: List[PartialPerson] = dataSource flatMap {
    case (n, a, e) =>
      PartialPerson(Some(n), Some(a), Some(e)) ::
        PartialPerson(Some(n), Some(a), None) ::
        PartialPerson(Some(n), None, Some(e)) ::
        PartialPerson(None, Some(a), Some(e)) ::
        PartialPerson(Some(n), None, None) ::
        PartialPerson(None, None, Some(e)) ::
        Nil
  }

  private val samplePartialPeopleWithAges: List[PartialPerson] = dataSource flatMap {
    case (n, a, e) =>
      PartialPerson(Some(n), Some(a), Some(e)) ::
        PartialPerson(Some(n), Some(a), None) ::
        PartialPerson(None, Some(a), Some(e)) ::
        Nil
  }

  private val samplePeopleWithPartialData: List[SamplePerson] = samplePartialPeople map (pp =>
    SamplePerson(pp.name orNull, pp.age getOrElse 0, pp.email.orNull))

  private val sampleSemiPartialPeople: List[SemiPartialPerson] = samplePartialPeople map { pp =>
    SemiPartialPerson(pp.name.orNull, pp.age, pp.email)
  }

  private lazy val testSourceDatasetWithPartialData = spark.createDataset(samplePartialPeople)

  private lazy val testH2oFrameWithPartialData: H2OFrame = hc.asH2OFrame(testSourceDatasetWithPartialData)

  test("Dataset[SamplePerson] to H2OFrame and back") {

    assertDatasetBasicProperties(testSourceDataset, testH2oFrame, row => {
      val sample = samplePeople(row.toInt)
      val value = testH2oFrame.collectStrings(0)(row.toInt)
      assert(sample.name == value, s"The H2OFrame values should match")
    }, List("name", "age", "email"))

    val extracted = readWholeFrame[SamplePerson](testH2oFrame)

    assert(testSourceDataset.count == testH2oFrame.numberOfRows, "Number of rows should match")

    matchData(extracted, samplePeople)
  }

  test("Dataset[Byte] to H2OFrame") {
    val ds = spark.range(3).map(_.toByte)
    val hf = hc.asH2OFrame(ds)
    assertVectorIntValues(hf.collectInts(0), Seq(0, 1, 2))
  }

  test("Dataset[Short] to H2OFrame") {
    val ds = spark.range(3).map(_.toShort)
    val hf = hc.asH2OFrame(ds)
    assertVectorIntValues(hf.collectInts(0), Seq(0, 1, 2))
  }

  test("Dataset[Int] to H2OFrame") {
    val ds = spark.range(3).map(_.toInt)
    val hf = hc.asH2OFrame(ds)
    assertVectorIntValues(hf.collectInts(0), Seq(0, 1, 2))
  }

  test("Dataset[Long] to H2OFrame") {
    val ds = spark.range(3).map(_.toLong)
    val hf = hc.asH2OFrame(ds)
    assertVectorIntValues(hf.collectInts(0), Seq(0, 1, 2))
  }

  test("Dataset[Float] to H2OFrame") {
    val ds = spark.range(3).map(_.toFloat)
    val hf = hc.asH2OFrame(ds)
    assertVectorDoubleValues(hf.collectDoubles(0), Seq(0, 1.0, 2.0))
  }

  test("Dataset[Double] to H2OFrame") {
    val ds = spark.range(3).map(_.toDouble)
    val hf = hc.asH2OFrame(ds)
    assertVectorDoubleValues(hf.collectDoubles(0), Seq(0, 1.0, 2.0))
  }

  test("Dataset[String] to H2OFrame") {
    val ds = spark.range(3).map(_.toString)
    val hf = hc.asH2OFrame(ds)
    assertVectorStringValues(hf.collectStrings(0), Seq("0", "1", "2"))
  }

  test("Dataset[Boolean] to H2OFrame") {
    val ds = spark.sparkContext.parallelize(Seq(true, false, true)).toDS()
    val hf = hc.asH2OFrame(ds)
    assertVectorIntValues(hf.collectInts(0), Seq(1, 0, 1))
  }

  test("Datasets with a type that does not match") {

    intercept[IllegalArgumentException] {
      hc.asRDD[SampleCompany](testH2oFrame)
      fail(s"Should not have accepted mismatching data")
    }
  }

  test("Datasets with two different class names, same structure") {
    checkWith((n, a, e) => WeirdPerson(e, a, n))
  }

  test("Datasets with two different class names and misplaced positions") {
    checkWith((n, a, e) => SampleAccount(e, n, a))
  }

  test("Datasets with a projection") {
    checkWith((n, a, _) => SampleCat(n, a))
  }

  test("Datasets with a projection to singletons") {
    checkWith((n, _, _) => SampleString(n))
  }

  test("Converting Total Dataset to Optional") {
    checkWith((n, a, e) => PartialPerson(Some(n), Some(a), Some(e)))
  }

  test("Dataset[PartialPerson] to H2OFrame and back") {

    val extracted = readWholeFrame[PartialPerson](testH2oFrameWithPartialData)

    assert(
      testSourceDatasetWithPartialData.count == testH2oFrameWithPartialData.numberOfRows,
      "Number of rows should match")

    matchData(extracted, samplePartialPeople)
  }

  test("Dataset[PartialPerson] - extracting SamplePersons with nulls") {
    try {
      val extracted = readWholeFrame[SamplePerson](testH2oFrameWithPartialData)
      println(extracted)
      matchData(extracted, samplePeopleWithPartialData)
      fail("Should have caught an error")
    } catch {
      case x: Exception =>
        assert(x.getMessage contains "column 1 value missing", s"Should have caught an error, got $x")
    }
  }

  test("Dataset[PartialPerson] - extracting SamplePersons with nulls only in strings") {
    try {
      lazy val testSourceDatasetWithPartialDataAgesPresent = spark.createDataset(samplePartialPeopleWithAges)

      val expected: List[SamplePerson] = samplePartialPeopleWithAges map (p =>
        SamplePerson(p.name.orNull, p.age.get, p.email.orNull))
      val extracted = readWholeFrame[SamplePerson](hc.asH2OFrame(testSourceDatasetWithPartialDataAgesPresent))

      matchData(extracted, expected)

    } catch {
      case x: Exception =>
        fail(x.getMessage)
    }
  }

  test("Dataset[PartialPerson] - extracting SemiPartialPersons should give something") {
    val rdd0 = hc.asRDD[PartialPerson](testH2oFrameWithPartialData)
    val c0 = rdd0.count()
    assert(c0 == 24)
    val rdd1 = hc.asRDD[SemiPartialPerson](testH2oFrameWithPartialData)
    val c1 = rdd1.count()
    assert(c1 > 0)
    val rdd2: RDD[SemiPartialPerson] = hc.asRDD[SemiPartialPerson](testH2oFrameWithPartialData)
    assert(rdd2.count() > 0)
    val asDS = rdd2.toDS()
    assert(asDS.count() > 0)
    val extracted = asDS.collect()
    assert(extracted.nonEmpty)
  }

  test("Dataset[PartialPerson] - extracting SemiPartialPersons") {
    val extracted = readWholeFrame[SemiPartialPerson](testH2oFrameWithPartialData)

    matchData(extracted, sampleSemiPartialPeople)
  }

  test("Dataset[PartialPerson] - extracting SampleCats") {
    lazy val testSourceDatasetWithPartialDataAgesPresent = spark.createDataset(samplePartialPeopleWithAges)

    val sampleCats =
      samplePartialPeopleWithAges.flatMap(p =>
        SampleCat(p.name.orNull, p.age.get) :: SampleCat(p.name.orNull, p.age.get) :: Nil)
    val extracted = readWholeFrame[SampleCat](hc.asH2OFrame(testSourceDatasetWithPartialDataAgesPresent))

    matchData(extracted, sampleCats) // the idea is, all sample people are there, the rest is ignored
  }

  private lazy val testSourceDataset = spark.createDataset(samplePeople)

  private lazy val testH2oFrame: H2OFrame = H2OFrame(hc.asH2OFrameKeyString(testSourceDataset))

  private def readWholeFrame[T <: Product: TypeTag: ClassTag](frame: H2OFrame) = {
    val asrdd: RDD[T] = hc.asRDD[T](frame)
    val asDS = asrdd.toDS()
    val extracted = asDS.collect()
    extracted
  }

  private def matchData[T <: Product](actual: Seq[T], expected: Seq[T]): Unit = {
    val extra = actual.diff(expected)
    assert(extra.isEmpty, s"Unexpected records: $extra")
    val missing = actual.diff(expected)
    assert(missing.isEmpty, s"Not found: $missing")
  }

  private def checkWith[T <: Product: TypeTag: ClassTag](constructor: (String, Int, String) => T): Unit = {
    val samples = dataSource map { case (n, a, e) => constructor(n, a, e) }
    val extracted = readWholeFrame[T](testH2oFrame)
    matchData(extracted, samples)
  }
}
