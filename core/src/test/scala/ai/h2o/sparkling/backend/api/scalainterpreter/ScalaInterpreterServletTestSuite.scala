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
import ai.h2o.sparkling.backend.exceptions.RestApiCommunicationException
import ai.h2o.sparkling.repl.CodeResults
import org.apache.spark.sql.SparkSession
import org.scalatest.FunSuite

class ScalaInterpreterServletTestSuite extends FunSuite with SharedH2OTestContext with ScalaInterpreterRestApi {

  override def createSparkSession(): SparkSession =
    sparkSession(
      "local[*]",
      defaultSparkConf
        .set("spark.ext.h2o.repl.enabled", "true")
        .set("spark.ext.scala.int.default.num", "2"))

  test("ScalaCodeHandler.initSession() method") {
    val result = initSession()
    assert(result.session_id == 1, "First id should be equal to 1")
    assert(getSessions().sessions.length == 1)
    destroySession(result.session_id)
  }

  test("ScalaCodeHandler.destroySession() method, destroy existing session") {
    val result = initSession()
    destroySession(result.session_id)
    assert(getSessions().sessions.length == 0)
  }

  test("ScalaCodeHandler.destroySession() method, destroy non-existing session") {
    intercept[RestApiCommunicationException] {
      destroySession(777) // ID 777 does not exist
    }
    assert(getSessions().sessions.length == 0)
  }

  test("ScalaCodeHandler.getSessions() method") {
    initSession()
    initSession()
    val result = getSessions()

    val actualSessionIds = result.sessions.sorted
    assert(
      actualSessionIds.sorted.sameElements(Array(1, 2)),
      s"Array of active sessions should contain 1 and 2, but it is [${actualSessionIds.mkString(",")}]")
    assert(getSessions().sessions.length == 2)
    getSessions().sessions.foreach(destroySession)
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
    val session1 = initSession()
    val result1 = interpret(session1.session_id, "case class Foo(num: Int)")
    assert(result1.output.equals(""), "Printed output should be empty")
    assert(result1.status.equals("Success"), "Status should be Success")
    assert(result1.response.equals("defined class Foo\n"), "Response should not be empty")
    destroySession(session1.session_id)

    val session2 = initSession()
    val result2 = interpret(session2.session_id, "val num = Foo(42)")
    assert(result2.output.equals(""), "Printed output should equal to text")
    assert(result2.status.equals("Success"), "Status should be Success")
    assert(result2.response.equals("num: Foo = Foo(42)\n"), "Response should not be empty")
    destroySession(session2.session_id)
  }

  test("ScalaCodeHandler.interpret() method, using sqlContext, h2oContext and sparkContext") {
    val session1 = initSession()
    val result1 = interpret(session1.session_id, "val rdd = sc.parallelize(1 to 100, 8).map(v=>v+10);rdd.cache")
    assert(result1.output.equals(""), "Printed output should be empty")
    assert(result1.status.equals("Success"), "Status should be Success ")
    assert(
      result1.response.contains("rdd: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD"),
      "Response should not be empty")
    destroySession(session1.session_id)

    val session2 = initSession()
    val result2 = interpret(session1.session_id, "val h2oFrame = h2oContext.asH2OFrame(rdd)")
    assert(result2.output.equals(""), "Printed output should be empty")
    assert(
      result2.status.equals("Success"),
      s"Status should be Success, got ${result2.status}, reason: ${result2.response} ")
    assert(!result2.response.equals(""), "Response should not be empty")
    destroySession(session2.session_id)

    val session3 = initSession()
    val result3 = interpret(session1.session_id, "val dataframe = h2oContext.asSparkFrame(h2oFrame)")
    assert(result3.output.equals(""), "Printed output should be empty")
    assert(result3.status.equals("Success"), "Status should be Success 3")
    assert(!result3.response.equals(""), "Response should not be empty")
    destroySession(session3.session_id)
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

  private def testCode(code: String, expectedResult: CodeResults.Value): ScalaCode = {
    val session = initSession()
    val result = interpret(session.session_id, code)
    assert(result.status == expectedResult.toString)
    destroySession(session.session_id)
    result
  }
}
