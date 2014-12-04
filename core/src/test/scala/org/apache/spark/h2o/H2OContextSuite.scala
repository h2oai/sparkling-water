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
import org.apache.spark.h2o.util.{SparkTestContext, LocalSparkClusterContext}
import org.scalatest.{BeforeAndAfter, Matchers, FunSuite}
import water.util.IcedInt
import water.{Key, DKV}

/**
 * Testing creation of H2O cloud in distributed environment.
 */
class H2OContextSuite extends FunSuite
  with Matchers with BeforeAndAfter with SparkTestContext {

  test("verify H2O cloud building on local JVM") {
    sc = new SparkContext("local", "test-local")
    hc = new H2OContext(sc).start()
    // Make sure that H2O is running
    assert(water.H2O.store_size() == 0)
    DKV.put(Key.make(), new IcedInt(43))
    assert(water.H2O.store_size() == 1)
    resetSparkContext()
  }

  test("verify H2O cloud building on local cluster") {
    // For distributed testing we need to pass around jar containing all implementation classes plus test classes
    val swassembly = sys.props.getOrElse("sparkling.test.assembly",
      fail("The variable 'sparkling.test.assembly' is not set! It should point to assembly jar file."))
    sc = new SparkContext("local-cluster[3,2,721]", "test-local-cluster", null, swassembly :: Nil)
    hc = new H2OContext(sc).start()
    //
    resetSparkContext()
  }


}
