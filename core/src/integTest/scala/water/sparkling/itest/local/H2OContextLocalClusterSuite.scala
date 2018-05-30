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
package water.sparkling.itest.local

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.{H2OContextTestHelper, SparkTestContext}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}

/**
 * Testing creation of H2O cloud in distributed environment.
 */
@RunWith(classOf[JUnitRunner])
class H2OContextLocalClusterSuite extends FunSuite
  with Matchers with BeforeAndAfter with SparkTestContext {

  val swassembly = sys.props.getOrElse("sparkling.assembly.jar",
    fail("The variable 'sparkling.assembly.jar' is not set! It should point to assembly jar file."))

  test("verify H2O cloud building on local cluster") {
    // For distributed testing we need to pass around jar containing all implementation classes plus test classes
    val conf = defaultSparkConf.setJars(swassembly :: Nil)
    sc = new SparkContext("local-cluster[1,2,2048]", "test-local-cluster", conf)

    val hc = H2OContextTestHelper.createH2OContext(sc, 1)
    assert(water.H2O.CLOUD.members().length == 1, "H2O cloud should have 3 members")

    H2OContextTestHelper.stopH2OContext(sc, hc)
    // Does not reset
    resetSparkContext()
  }
}
