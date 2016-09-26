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
import org.apache.spark.h2o.{H2OConf, H2OContext}
import org.apache.spark.h2o.utils.SparkTestContext
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfter, FunSuite, Matchers}
import water.util.IcedInt
import water.{DKV, Iced, Key}

/**
 * Testing creation of H2O cloud in distributed environment.
 */
@RunWith(classOf[JUnitRunner])
class H2OContextLocalSuite extends FunSuite
  with Matchers with BeforeAndAfter with SparkTestContext {

  test("verify H2O cloud building on local JVM") {
    sc = new SparkContext("local[*]", "test-local", defaultSparkConf)
    
    // start h2o cloud in case of external cluster mode
    if(testsInExternalMode(sc.getConf)){
      startCloud(1, sc.getConf)
    }
    hc = H2OContext.getOrCreate(sc, new H2OConf(sc).setNumOfExternalH2ONodes(1))

    // Number of nodes should be on
    assert(water.H2O.CLOUD.members().length == 1, "H2O cloud should have 1 members")
    // Make sure that H2O is running
    assert(water.H2O.store_size() == 0)
    // We need to do this magic since H2O's IcedInt does not propagate self type properly
    val icedInt: Iced[IcedInt] = new IcedInt(43).asInstanceOf[Iced[IcedInt]]
    DKV.put(Key.make(), icedInt)
    assert(water.H2O.store_size() == 1)

    // stop h2o cloud in case of external cluster mode
    if(testsInExternalMode(sc.getConf)){
      stopCloud()
    }
    // Reset this context
    resetContext()
  }
}
