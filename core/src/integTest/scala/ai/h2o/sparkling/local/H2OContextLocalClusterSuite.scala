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
package ai.h2o.sparkling.local

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SharedH2OTestContext
import org.apache.spark.h2o.{H2OConf, H2OContext}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
 * Testing creation of H2O cloud in distributed environment.
 */
@RunWith(classOf[JUnitRunner])
class H2OContextLocalClusterSuite extends FunSuite with SharedH2OTestContext {

  private val swassembly = sys.props.getOrElse("sparkling.assembly.jar",
    fail("The variable 'sparkling.assembly.jar' is not set! It should point to assembly jar file."))

  override def createSparkContext: SparkContext = new SparkContext("local", "test-local-cluster", defaultSparkConf.setJars(swassembly :: Nil))


  test("verify H2O cloud building on local cluster") {
    // For distributed testing we need to pass around jar containing all implementation classes plus test classes
    val hc = H2OContext.getOrCreate(new H2OConf().setClusterSize(1))
    assert(water.H2O.CLOUD.members().length == 1, "H2O cloud should have 1 member")
  }

}
