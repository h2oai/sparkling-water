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

package ai.h2o.sparkling.ml.param

import ai.h2o.automl.Algo
import ai.h2o.sparkling.ml.params.EnumParamValidator._
import hex.genmodel.utils.DistributionFamily
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class EnumParamValidatorTestSuite extends FunSuite with Matchers {

  test("getValidatedEnumValue with unknown enum") {
    val thrown = intercept[IllegalArgumentException] {
      getValidatedEnumValue[DistributionFamily]("not_exist")
    }
    assert(thrown.getMessage.startsWith("'not_exist' is not a valid value. Allowed values are:"))
  }

  test("getValidatedEnumValue with null") {
    val thrown = intercept[IllegalArgumentException] {
      getValidatedEnumValue[DistributionFamily](null)
    }
    assert(thrown.getMessage.startsWith("Null is not a valid value. Allowed values are:"))
  }

  test("getValidatedEnumValue with valid enum value") {
    val ret = getValidatedEnumValue[DistributionFamily]("gaussian")
    assert(ret == "gaussian")
  }

  test("getValidatedEnumValue with valid enum value, different case then the enum") {
    val ret = getValidatedEnumValue[DistributionFamily]("gAuSsiAn")
    assert(ret == "gaussian")
  }

  test("getValidatedEnumValues with null & null is enabled") {
    val ret = getValidatedEnumValues[Algo](null, nullEnabled = true)
    assert(ret == null)
  }

  test("getValidatedEnumValues with null & null not enabled") {
    val thrown = intercept[IllegalArgumentException] {
      getValidatedEnumValues[Algo](null)
    }
    assert(thrown.getMessage.startsWith("Null is not a valid value"))
  }

  test("getValidatedEnumValues with empty array") {
    val ret = getValidatedEnumValues[Algo](Array.empty[String])
    assert(ret.sameElements(Array.empty[String]))
    assert(ret.length == 0)
  }

  test("getValidatedEnumValues with valid values") {
    val algos = Array("GBM", "DeepLearning")
    val ret = getValidatedEnumValues[Algo](algos)
    assert(ret.sameElements(algos))
    assert(ret.length == 2)
  }

  test("getValidatedEnumValues with valid values, different case") {
    val ret = getValidatedEnumValues[Algo](Array("GbM", "DEEPLearning"))
    assert(ret.sameElements(Array("GBM", "DeepLearning")))
    assert(ret.length == 2)
  }

  test("getValidatedEnumValues with one invalid value") {
    val thrown = intercept[IllegalArgumentException] {
      getValidatedEnumValues[Algo](Array("GbM", "not_exist"))
    }
    assert(thrown.getMessage.startsWith("'not_exist' is not a valid value"))
  }

  test("getValidatedEnumValues with one null value") {
    val thrown = intercept[IllegalArgumentException] {
      getValidatedEnumValues[Algo](Array("GbM", null))
    }
    assert(thrown.getMessage.startsWith("Null can not be specified as the input array element"))
  }
}
