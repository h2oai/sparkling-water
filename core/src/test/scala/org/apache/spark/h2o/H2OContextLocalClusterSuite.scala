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
package org.apache.spark.h2o

import org.apache.spark.SparkContext
import org.apache.spark.h2o.util.SparkTestContext
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import water.{DKV, Key}
import org.junit.runner.RunWith

/**
 * Testing creation of H2O cloud in distributed environment.
 */
@RunWith(classOf[JUnitRunner])
class H2OContextLocalClusterSuite extends FunSuite
  with Matchers with BeforeAndAfter with SparkTestContext {

  val swassembly = sys.props.getOrElse("sparkling.test.assembly",
    fail("The variable 'sparkling.test.assembly' is not set! It should point to assembly jar file."))

  test("verify H2O cloud building on local cluster") {
    // For distributed testing we need to pass around jar containing all implementation classes plus test classes
    val conf = defaultSparkConf.setJars(swassembly :: Nil)
    sc = new SparkContext("local-cluster[3,2,1024]", "test-local-cluster", conf)
    hc = H2OContext.getOrCreate(sc)

    assert(water.H2O.CLOUD.members().length == 3, "H2O cloud should have 3 members")
    // Does not reset
    resetContext()
  }

  // IGNORED since we are not able to initialize client in the process several times
  ignore("2nd run to verify that test does not overlap") {
    val conf = defaultSparkConf.setJars(swassembly :: Nil)
    sc = new SparkContext("local-cluster[3,2,721]", "test-local-cluster", conf)
    hc = H2OContext.getOrCreate(sc)

    resetContext()
  }
}
