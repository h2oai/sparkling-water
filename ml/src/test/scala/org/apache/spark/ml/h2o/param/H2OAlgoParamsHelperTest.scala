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