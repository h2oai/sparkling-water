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
package org.apache.spark.repl.h2o

import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest._

import scala.util.matching.Regex

/**
 * Test that patcher is patching.
 */
@RunWith(classOf[JUnitRunner])
class PatchUtilsTestSuite extends FunSuite with BeforeAndAfterAll {

  val EXAMPLE_CLASS_NAME = "intp_id_12$line1.$read$$iw$$iw"
  val EXAMPLE_RESULT_AFTER_PATCH = "intp_id_12$line1.$read"
  val FAILED_MATCH = "FAIL"

  def assertMatch(regex: Regex, input: String, exp: String):Unit = {
    val result = input match {
      case regex(b) => b
      case _ => FAILED_MATCH
    }
    assert(result == exp)
  }

  test("Test new regexp for OuterScopes") {
    val regex = PatchUtils.OUTER_SCOPE_REPL_REGEX
    assertMatch(regex, EXAMPLE_CLASS_NAME, EXAMPLE_RESULT_AFTER_PATCH)
    assertMatch(regex, "$line1.$read$$iw$$iw", "$line1.$read")
  }

  test("[SW-386] Test patched OuterScopes") {
    val regexBeforePatch = getRegexp()
    // Default regexp fails for our class names with intp_id prefix
    assertMatch(regexBeforePatch, EXAMPLE_CLASS_NAME, FAILED_MATCH)

    PatchUtils.PatchManager.patch("SW-386", Thread.currentThread().getContextClassLoader)

    val regexAfterPatch = getRegexp()
    assertMatch(regexAfterPatch, EXAMPLE_CLASS_NAME, EXAMPLE_RESULT_AFTER_PATCH)
  }

  def getRegexp(): Regex = {
    val clz = Class.forName(PatchUtils.OUTER_SCOPES_CLASS + "$")
    val module = PatchUtils.getModule(clz)
    val f = clz.getDeclaredField("REPLClass")
    f.setAccessible(true)
    f.get(module).asInstanceOf[Regex]
  }
}
