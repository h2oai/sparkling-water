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
package water.api

import org.apache.spark.SparkContext
import org.apache.spark.h2o._
import org.apache.spark.h2o.util.SparkTestContext
import org.apache.spark.sql.SQLContext
import org.junit.Ignore
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.api.scalaInt._

/**
 * Test suite for scalaint end-points
 */
@RunWith(classOf[JUnitRunner])
@Ignore
class ScalaCodeHandlerSuite extends FunSuite with SparkTestContext {
  sc = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)
  hc = H2OContext.getOrCreate(sc)
  // Shared h2oContext
  val h2oContext = hc
  // Shared sqlContext
  implicit val sqlContext = new SQLContext(sc)

  test("ScalaCodeHandler.initSession() method"){

    val scalaCodeHandler = new ScalaCodeHandler(sc,h2oContext)

    val req = new ScalaSessionIdV3
    val result = scalaCodeHandler.initSession(3,req)

    assert(result.session_id == 1,"First id should be equal to 1")
    // new interpreter is automatically created, so the last ID used should be equal to 2
    assert(scalaCodeHandler.mapIntr.size == 1, "Number of currently used interpreters should be equal to 1")
    assert(scalaCodeHandler.mapIntr.get(1).nonEmpty, "The value in the interpreters hashmap with the key 1 should not be empty")
    assert(scalaCodeHandler.mapIntr.get(1).get._1.sessionID == 1, "ID attached to the interpreter should be equal to 1")
  }

  test("ScalaCodeHandler.destroySession() method, destroy existing session"){
    val scalaCodeHandler = new ScalaCodeHandler(sc,h2oContext)

    // create new session
    val reqSession = new ScalaSessionIdV3
    scalaCodeHandler.initSession(3,reqSession)

    val reqMsg = new ScalaMsgV3
    reqMsg.session_id=reqSession.session_id
    val result = scalaCodeHandler.destroySession(3,reqMsg)

    assert(result.msg.equals("Session closed"),"Message should be equal to \"Session closed\"")
    assert(scalaCodeHandler.mapIntr.size == 0, "Number of currently used interpreters should be equal to 0")
    assert(scalaCodeHandler.mapIntr.get(1).isEmpty, "The value in the interpreters hashmap with the key 1 should be empty")
  }

  test("ScalaCodeHandler.destroySession() method, destroy non-existing session"){
    val scalaCodeHandler = new ScalaCodeHandler(sc,h2oContext)

    val reqMsg = new ScalaMsgV3
    reqMsg.session_id=3
    val result = scalaCodeHandler.destroySession(3,reqMsg)

    assert(result.msg.equals("Session does not exist"),"Message should be equal to \"Session does not exist\"")
    assert(scalaCodeHandler.mapIntr.size == 0, "Number of currently used interpreters should be equal to 0")
    assert(scalaCodeHandler.mapIntr.get(3).isEmpty, "The value in the interpreters hashmap with the key 3 should be empty")
  }

  test("ScalaCodeHandler.getSessions() method"){
    val scalaCodeHandler = new ScalaCodeHandler(sc,h2oContext)

    // create first interpreter
    val reqSession1 = new ScalaSessionIdV3
    scalaCodeHandler.initSession(3,reqSession1)

    // create second interpreter
    val reqSession2 = new ScalaSessionIdV3
    scalaCodeHandler.initSession(3,reqSession2)

    val req = new ScalaSessionsV3
    val result = scalaCodeHandler.getSessions(3,req)

    assert(result.sessions.sameElements(Array(1,2)),"Array of active sessions should contain 1 and 2")
    assert(scalaCodeHandler.mapIntr.size == 2, "Number of currently used interpreters should be equal to 2")
  }

  test("ScalaCodeHandler.interpret() method, printing"){
    val scalaCodeHandler = new ScalaCodeHandler(sc,h2oContext)

    // create interpreter
    val reqSession = new ScalaSessionIdV3
    scalaCodeHandler.initSession(3,reqSession)


    val req = new ScalaCodeV3
    req.session_id = reqSession.session_id
    req.code = "println(\"text\")"
    val result = scalaCodeHandler.interpret(3,req)

    assert(result.output.equals("text\n"),"Printed output should equal to text")
    assert(result.status.equals("Success"),"Status should be Success")
    assert(result.response.equals(""),"Response should be empty")
  }

  test("ScalaCodeHandler.interpret() method, using unknown function"){
    val scalaCodeHandler = new ScalaCodeHandler(sc,h2oContext)

    // create interpreter
    val reqSession = new ScalaSessionIdV3
    scalaCodeHandler.initSession(3,reqSession)


    val req = new ScalaCodeV3
    req.session_id = reqSession.session_id
    req.code = "foo"
    val result = scalaCodeHandler.interpret(3,req)

    assert(result.output.equals(""),"Printed output should be empty")
    assert(result.status.equals("Error"),"Status should be Error")
    assert(result.response.equals("<console>:29: error: not found: value foo\n              foo\n              ^\n"),"Response should not be empty")
  }

  test("ScalaCodeHandler.interpret() method, using previously defined class"){
    val scalaCodeHandler = new ScalaCodeHandler(sc,h2oContext)

    // create interpreter
    val reqSession = new ScalaSessionIdV3
    scalaCodeHandler.initSession(3,reqSession)

    val req1 = new ScalaCodeV3
    req1.session_id = reqSession.session_id
    req1.code = "case class Foo(num: Int)"
    val result1 = scalaCodeHandler.interpret(3,req1)

    assert(result1.output.equals(""),"Printed output should be empty")
    assert(result1.status.equals("Success"),"Status should be Success")
    assert(result1.response.equals("defined class Foo\n"),"Response should not be empty")

    val req2= new ScalaCodeV3
    req2.session_id = reqSession.session_id
    req2.code = "val num = Foo(42)"
    val result2 = scalaCodeHandler.interpret(3,req2)

    assert(result2.output.equals(""),"Printed output should equal to text")
    assert(result2.status.equals("Success"),"Status should be Success")
    assert(result2.response.equals("num: Foo = Foo(42)\n"),"Response should not be empty")
  }


  test("ScalaCodeHandler.interpret() method, using sqlContext,h2oContext and sparkContext"){
    val scalaCodeHandler = new ScalaCodeHandler(sc,h2oContext)

    // create interpreter
    val reqSession = new ScalaSessionIdV3
    scalaCodeHandler.initSession(3,reqSession)

    val req1 = new ScalaCodeV3
    req1.session_id = reqSession.session_id
    req1.code = "val rdd = sc.parallelize(1 to 100).map(v=>v+10);rdd.cache"
    val result1 = scalaCodeHandler.interpret(3,req1)
    assert(result1.output.equals(""),"Printed output should be empty")
    assert(result1.status.equals("Success"),"Status should be Success")
    assert(result1.response
             .equals("rdd: org.apache.spark.rdd.RDD[Int] = MapPartitionsRDD[5] at map at <console>:28\n" +
                     "res0: rdd.type = MapPartitionsRDD[5] at map at <console>:28\n"),
           "Response should not be empty")

    val req2 = new ScalaCodeV3
    req2.session_id = reqSession.session_id
    req2.code = "val h2oFrame = h2oContext.asH2OFrame(rdd)"
    val result2 = scalaCodeHandler.interpret(3,req2)
    assert(result2.output.equals(""),"Printed output should be empty")
    assert(result2.status.equals("Success"),"Status should be Success")
    assert(!result2.response.equals(""),"Response should not be empty")

    val req3 = new ScalaCodeV3
    req3.session_id = reqSession.session_id
    // this code is using implicitly sqlContext
    req3.code = "val dataframe = h2oContext.asDataFrame(h2oFrame)"
    val result3 = scalaCodeHandler.interpret(3,req3)
    assert(result3.output.equals(""),"Printed output should be empty")
    assert(result3.status.equals("Success"),"Status should be Success")
    assert(!result3.response.equals(""),"Response should not be empty")
  }
}
