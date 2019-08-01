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

package ai.h2o.sparkling.macros

import org.apache.spark.expose.Logging
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner


@RunWith(classOf[JUnitRunner])
class DeprecatedMethodMacroSuite extends FunSuite with Logging {

  class VariousAnnotationUsages {
    @DeprecatedMethod(replacement = "replacing method")
    def method1: Unit = println("method1")

    @DeprecatedMethod("replacing method")
    def method2: Unit = println("mehtod2")

    @DeprecatedMethod()
    def method3: Unit = println("method3")

    @DeprecatedMethod
    def method4: Unit = println("mehtod4")
  }

  test("Various annotation usages") {
    val vau = new VariousAnnotationUsages()
    vau.method1
    vau.method2
    vau.method3
    vau.method4
  }

  class VariousAnnotatedMethods {
    @DeprecatedMethod(replacement = "replacing method")
    def method1: String = "method1"

    @DeprecatedMethod(replacement = "replacing method")
    def method2(parameter: String): Unit = println(parameter)

    @DeprecatedMethod(replacement = "replacing method")
    def method3[T](parameter: T): T = parameter

    @DeprecatedMethod(replacement = "replacing method")
    private[DeprecatedMethodMacroSuite] def method4: Unit = println("mehtod4")
  }

  test("Various annotated methods") {
    val vam = new VariousAnnotatedMethods()
    vam.method1
    vam.method2("method2")
    vam.method3("method3")
    vam.method4
  }
}
