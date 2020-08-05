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

package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.SparkTestContext
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatestplus.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

@RunWith(classOf[JUnitRunner])
class BinaryModelTestSuiteWithoutH2ORuntime extends FunSuite with Matchers with SparkTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  test("Using Binary model when H2O Runtime is not available throws exception") {
    val thrown = intercept[IllegalArgumentException] {
      H2OBinaryModel.read("/path/not/required")
    }
    assert(thrown.getMessage == "To use features available on a binary model, H2O Context has to be running!")
  }

}
