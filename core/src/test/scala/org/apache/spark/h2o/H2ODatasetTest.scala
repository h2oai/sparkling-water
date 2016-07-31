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
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import water.fvec.Vec
import water.parser.BufferedString

/**
  * Testing schema for h2o schema spark dataset transformation.
  */

case class SamplePerson(name: String, age: Int, email: String)
case class WeirdPerson(email: String, age: Int, name: String)
case class SampleCompany(officialName: String, count: Int, url: String)
case class SampleAccount(email: String, name: String, age: Int)
case class SampleCat(name: String, age: Int)

@RunWith(classOf[JUnitRunner])
class H2ODatasetTest extends FunSuite with SharedSparkTestContext with BeforeAndAfterAll {

  val dataSource =
      ("Hermione Granger", 15, "hgranger@griffindor.edu.uk") ::
      ("Ron Weasley", 14, "rweasley@griffindor.edu.uk") ::
      ("Harry Potter", 14, "hpotter@griffindor.edu.uk") ::
      ("Lucius Malfoy", 13, "lucius@slitherin.edu.uk") :: Nil

  val samplePeople: List[SamplePerson] = dataSource map SamplePerson.tupled

  lazy val sqlContext = hsc.sqlContext
  lazy val testSourceDataset = {
    import sqlContext.implicits._
    sqlContext.createDataset(samplePeople)
  }

  def testH2oFrame: H2OFrame = hsc.asH2OFrame(testSourceDataset)

  import scala.reflect.ClassTag
  import scala.reflect.runtime.universe._

  def readWholeFrame[T <: Product : TypeTag : ClassTag](frame: H2OFrame) = {

    import sqlContext.implicits._

    val asrdd: RDD[T] = hc.asRDD[T](frame)
    val asDS = asrdd.toDS()
    val extracted = asDS.collect()
    extracted
  }

  //  def extractAll[T] = {
  //    val asrdd: RDD[SamplePerson] = hc.asRDD[SamplePerson](h2oFrame)
  //    val asDS = asrdd.toDS()
  //    val extracted = asDS.collect()
  //  }

  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)

  override def afterAll(): Unit = {
    testH2oFrame.delete()
    testSourceDataset.unpersist()
  }

  def matchData[T](actual: Seq[T], expected:Seq[T]): Unit = {
    for {p <- expected} assert(actual.contains(p), s"$p not found in the result $actual")
    for {p <- actual} assert(expected.contains(p), s"Did not expect $p in the result $expected")
    assert(actual.toSet == expected.toSet, "Oops, did not match the people")
  }

  def checkWith[T <: Product: TypeTag: ClassTag](cons: (String, Int, String) => T): Unit = {
    val samples = dataSource map { case (n, a, e) => cons(n,a,e) }
    val extracted = readWholeFrame[T](testH2oFrame)
    matchData(extracted, samples)
  }

  test("Dataset[SamplePerson] to H2OFrame and back") {

    assertBasicInvariants(testSourceDataset, testH2oFrame, (row, vec) => {
      val sample = samplePeople(row.toInt)
      val valueString = new BufferedString()

      val value = vec.atStr(valueString, row)
      assert(sample.name == value.toString, s"The H2OFrame values should match")
    }, List("name", "age", "email"))

    val extracted = readWholeFrame[SamplePerson](testH2oFrame)

    assert(testSourceDataset.count == testH2oFrame.numRows(), "Number of rows should match")

    matchData(extracted, samplePeople)
  }

  test("Datasets with a type that does not match") {

    intercept[IllegalArgumentException] {
      hc.asRDD[SampleCompany](testH2oFrame)
      fail(s"Should not have accepted mismatching data")
    }
  }

  test("Datasets with two different types") {
    checkWith ((n, a, e) => WeirdPerson(e, a, n))
  }

  test("Datasets with two different types and misplaced positions") {
    checkWith ((n, a, e) => SampleAccount(e, n, a))
  }

  test("Datasets with a projection") {
    checkWith ((n, a, e) => SampleCat(n, a))
  }

  private type RowValueAssert = (Long, Vec) => Unit

  private def assertBasicInvariants[T <: Product](ds: Dataset[T], df: H2OFrame, rowAssert: RowValueAssert, names: List[String]): Unit = {
    assertHolderProperties(df, names)
    assert(ds.count == df.numRows(), "Number of rows in H2OFrame and Dataset should match")

    val vec = df.vec(0)
    for (row <- Range(0, df.numRows().toInt)) {
      assert(!vec.isNA(row), "The H2OFrame should not contain any NA values")
      rowAssert(row, vec)
    }
  }

  private def assertHolderProperties(df: H2OFrame, names: List[String]): Unit = {
    val actualNames = df.names().toList
    val numCols = names.length
    assert(df.numCols() == numCols, s"H2OFrame should contain $numCols column(s), have ${df.numCols()}")
    assert(df.names().length == numCols, s"H2OFrame column names should be $numCols in size, have ${df.names().length}")
    assert(actualNames.equals(names),
      s"H2OFrame column names should be $names since Holder object was used to define Dataset, but it is $actualNames")
  }
}
