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

package org.apache.spark.ml.h2o.param

import hex.genmodel.utils.DistributionFamily
import org.apache.spark.ml.h2o.param.H2OAlgoParamsHelper.getValidatedEnumValue
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class H2OAlgoParamsHelperTest extends FunSuite with Matchers {

  test("getValidatedEnumValue with unknown enum") {
    val thrown = intercept[IllegalArgumentException] {
      getValidatedEnumValue[DistributionFamily]("not_exist")
    }
    assert(thrown.getMessage.startsWith("'not_exist' is not a valid value. Allowed values are:"))
  }

  test("getValidatedEnumValue with null, but null not allowed") {
    val thrown = intercept[IllegalArgumentException] {
      getValidatedEnumValue[DistributionFamily](null)
    }
    assert(thrown.getMessage.startsWith("Null is not a valid value. Allowed values are:"))
  }

  test("getValidatedEnumValue with null") {
    val ret = getValidatedEnumValue[DistributionFamily](null, nullAllowed = true)
    assert(ret == null)
  }

  test("getValidatedEnumValue with valid enum value") {
    val ret = getValidatedEnumValue[DistributionFamily]("gaussian")
    assert(ret == "gaussian")
  }

  test("getValidatedEnumValue with valid enum value, different case then the enum") {
    val ret = getValidatedEnumValue[DistributionFamily]("gAuSsiAn")
    assert(ret == "gaussian")
  }
}
