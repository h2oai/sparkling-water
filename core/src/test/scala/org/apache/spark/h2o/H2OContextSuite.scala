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

/**
 * Testing creation of H2O cloud in distributed environment.
 */
class H2OContextDistribSuite extends FunSuite
  with Matchers with BeforeAndAfter with SparkTestContext {

  test("verify H2O cloud building") {
    sc = new SparkContext("local", "test")
    hc = new H2OContext(sc).start()
    //
    resetSparkContext()
  }
}
