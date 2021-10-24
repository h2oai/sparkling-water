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
package ai.h2o.sparkling.repl

import ai.h2o.sparkling.SharedH2OTestContext
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class H2OInterpreterTestSuite extends FunSuite with SharedH2OTestContext with Matchers {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  test("should create correct settings") {
    val objectUnderTest = new H2OInterpreter(sc, hc, 1)
    val result = objectUnderTest.createSettings()
    result.Yreplsync.value shouldBe true
    result.Yreplclassbased.value shouldBe true
    result.usejavacp.value shouldBe true
    result.Yreploutdir.value shouldBe H2OInterpreter.classOutputDirectory.getAbsolutePath
  }

}
