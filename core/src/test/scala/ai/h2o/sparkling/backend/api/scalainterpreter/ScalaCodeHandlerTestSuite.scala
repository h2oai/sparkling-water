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
package ai.h2o.sparkling.backend.api.scalainterpreter

import ai.h2o.sparkling.SharedH2OTestContext
import ai.h2o.sparkling.repl.CodeResults
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterEach, FunSuite}
import water.exceptions.H2ONotFoundArgumentException

@RunWith(classOf[JUnitRunner])
class ScalaCodeHandlerTestSuite extends FunSuite with SharedH2OTestContext with BeforeAndAfterEach {

  private var scalaCodeHandler: ScalaCodeHandler = _

  override def createSparkSession(): SparkSession =
    sparkSession(
      "local[*]",
      defaultSparkConf
        .set("spark.ext.h2o.repl.enabled", "true")
        .set("spark.ext.scala.int.default.num", "2"))

  override protected def beforeEach(): Unit = {
    scalaCodeHandler = new ScalaCodeHandler(sc, hc)
  }

  test("ScalaCodeHandler after initialization") {
    assert(scalaCodeHandler.mapIntr.isEmpty, "Number of currently used interpreters should be equal to 0")
    assert(
      scalaCodeHandler.freeInterpreters.size() == 2,
      "Number of prepared but not used interpreters should be equal to 1")
  }

  test("ScalaCodeHandler.initSession() method") {
    val req = new ScalaSessionIdV3
    val result = scalaCodeHandler.initSession(3, req)

    assert(result.session_id == 1, "First id should be equal to 1")
    // new interpreter is automatically created, so the last ID used should be equal to 2
    assert(scalaCodeHandler.mapIntr.size == 1, "Number of currently used interpreters should be equal to 1")
    assert(
      scalaCodeHandler.mapIntr.get(1).nonEmpty,
      "The value in the interpreters hash map with the key 1 should not be empty")
    assert(scalaCodeHandler.mapIntr(1).sessionId == 1, "ID attached to the interpreter should be equal to 1")
  }

  test("ScalaCodeHandler.destroySession() method, destroy existing session") {
    // create new session
    val reqSession = new ScalaSessionIdV3
    scalaCodeHandler.initSession(3, reqSession)

    val reqMsg = new ScalaSessionIdV3
    reqMsg.session_id = reqSession.session_id
    scalaCodeHandler.destroySession(3, reqMsg)
    assert(scalaCodeHandler.mapIntr.isEmpty, "Number of currently used interpreters should be equal to 0")
    assert(
      scalaCodeHandler.mapIntr.get(1).isEmpty,
      "The value in the interpreters hashmap with the key 1 should be empty")
  }

  test("ScalaCodeHandler.destroySession() method, destroy non-existing session") {
    val reqMsg = new ScalaSessionIdV3
    reqMsg.session_id = 3
    intercept[H2ONotFoundArgumentException] {
      scalaCodeHandler.destroySession(3, reqMsg)
    }
    assert(scalaCodeHandler.mapIntr.isEmpty, "Number of currently used interpreters should be equal to 0")
    assert(
      scalaCodeHandler.mapIntr.get(3).isEmpty,
      "The value in the interpreters hashmap with the key 3 should be empty")
  }

  test("ScalaCodeHandler.getSessions() method") {
    // create first interpreter
    val reqSession1 = new ScalaSessionIdV3
    scalaCodeHandler.initSession(3, reqSession1)

    // create second interpreter
    val reqSession2 = new ScalaSessionIdV3
    scalaCodeHandler.initSession(3, reqSession2)

    val req = new ScalaSessionsV3
    val result = scalaCodeHandler.getSessions(3, req)

    val actualSessionIds = result.sessions.sorted
    assert(
      actualSessionIds.sorted.sameElements(Array(1, 2)),
      s"Array of active sessions should contain 1 and 2, but it is [${actualSessionIds.mkString(",")}]")
    assert(scalaCodeHandler.mapIntr.size == 2, "Number of currently used interpreters should be equal to 2")
  }

  test("ScalaCodeHandler.interpret() method, printing") {
    val result = testCode("println(\"text\")", CodeResults.Success)
    assert(result.output.equals("text\n"), "Printed output should be equal to \"text\"")
    assert(result.response.equals(""), "Response should be empty")
  }

  test("ScalaCodeHandler.interpret() method, using unknown function") {
    val result = testCode("foo", CodeResults.Error)
    assert(result.output.equals(""), "Printed output should be empty")
    assert(result.response.contains(" error: not found: value foo"), s"Response was: ${result.response}")
  }

  test("ScalaCodeHandler.interpret() method, using previously defined class") {
    // create interpreter
    val reqSession = new ScalaSessionIdV3
    scalaCodeHandler.initSession(3, reqSession)

    val req1 = new ScalaCodeV3
    req1.session_id = reqSession.session_id
    req1.code = "case class Foo(num: Int)"
    val result1 = scalaCodeHandler.interpret(3, req1)

    assert(result1.output.equals(""), "Printed output should be empty")
    assert(result1.status.equals("Success"), "Status should be Success")
    assert(result1.response.equals("defined class Foo\n"), "Response should not be empty")

    val req2 = new ScalaCodeV3
    req2.session_id = reqSession.session_id
    req2.code = "val num = Foo(42)"
    val result2 = scalaCodeHandler.interpret(3, req2)

    assert(result2.output.equals(""), "Printed output should equal to text")
    assert(result2.status.equals("Success"), "Status should be Success")
    assert(result2.response.equals("num: Foo = Foo(42)\n"), "Response should not be empty")
  }

  test("ScalaCodeHandler.interpret() method, using sqlContext, h2oContext and sparkContext") {
    // create interpreter
    val reqSession = new ScalaSessionIdV3
    scalaCodeHandler.initSession(3, reqSession)
    val req1 = new ScalaCodeV3
    req1.session_id = reqSession.session_id
    req1.code = "val rdd = sc.parallelize(1 to 100, 8).map(v=>v+10);rdd.cache"
    val result1 = scalaCodeHandler.interpret(3, req1)
    assert(result1.output.equals(""), "Printed output should be empty")
    assert(result1.status.equals("Success"), "Status should be Success ")
    assert(
      result1.response.contains("rdd: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD"),
      "Response should not be empty")

    val req2 = new ScalaCodeV3
    req2.session_id = reqSession.session_id
    req2.code = "val h2oFrame = h2oContext.asH2OFrame(rdd)"
    val result2 = scalaCodeHandler.interpret(3, req2)
    assert(result2.output.equals(""), "Printed output should be empty")
    assert(
      result2.status.equals("Success"),
      s"Status should be Success, got ${result2.status}, reason: ${result2.response} ")
    assert(!result2.response.equals(""), "Response should not be empty")

    val req3 = new ScalaCodeV3
    req3.session_id = reqSession.session_id
    // this code is using implicitly sqlContext
    req3.code = "val dataframe = h2oContext.asSparkFrame(h2oFrame)"
    val result3 = scalaCodeHandler.interpret(3, req3)
    assert(result3.output.equals(""), "Printed output should be empty")
    assert(result3.status.equals("Success"), "Status should be Success 3")
    assert(!result3.response.equals(""), "Response should not be empty")
  }

  test("Code with exception") {
    testCode("throw new Exception(\"Exception Message\")", CodeResults.Exception)
  }

  test("Incomplete code") {
    testCode("val num = ", CodeResults.Incomplete)
  }

  test("Simple script which ends successfully") {
    testCode("val num = 42", CodeResults.Success)
  }

  test("Test successful call after exception occurred") {
    testCode("throw new Exception(\"Exception Message\")", CodeResults.Exception)
    testCode("val num = 42", CodeResults.Success)
  }

  test("Test Spark API call via interpreter") {
    testCode(
      """
        |val list = Seq(('A', 1), ('B', 2), ('A', 3))
        |val num1 = sc.parallelize(list, 3).groupByKey.count
        |val num2 = sc.parallelize(list, 3).reduceByKey(_ + _).count
        |""".stripMargin,
      CodeResults.Success)
  }

  test(
    "[SW-386] Test Spark API exposed implicit conversions " +
      "(https://issues.scala-lang.org/browse/SI-9734 and https://issues.apache.org/jira/browse/SPARK-13456)") {
    testCode(
      """
        |import spark.implicits._
        |case class Person(id: Long)
        |val ds = Seq(Person(0), Person(1)).toDS
        |val count = ds.count
      """.stripMargin,
      CodeResults.Success)
  }

  private def testCode(code: String, expectedResult: CodeResults.Value): ScalaCodeV3 = {
    val reqSession = new ScalaSessionIdV3
    scalaCodeHandler.initSession(3, reqSession)
    val req = new ScalaCodeV3
    req.session_id = reqSession.session_id
    req.code = code
    val result = scalaCodeHandler.interpret(3, req)
    assert(result.status == expectedResult.toString)
    result
  }
}
