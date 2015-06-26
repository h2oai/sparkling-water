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
package water.api

import org.apache.spark.SparkContext
import org.apache.spark.h2o.util.SharedSparkTestContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner
import water.api.RDDs.{RDDsHandler, RDDsV3}

/**
 * Test method of RDDsHandler.
 */
@RunWith(classOf[JUnitRunner])
class RDDsHandlerSuite extends FunSuite with SharedSparkTestContext {

  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-local")

  test("RDDsHandler.list() method") {
    val rname = "Test"
    val rpart = 21
    val rdd = sc.parallelize(1 to 10, rpart).setName(rname).cache()

    val rddsHandler = new RDDsHandler(sc)
    val result = rddsHandler.list(3, new RDDsV3)
    assert (result.rdds.length == 1, "Number of created and persisted RDDs should be 1")
    assert (result.rdds(0).name.equals(rname), "Name matches")
    assert (result.rdds(0).partitions == rpart, "Number of partitions matches")

  }
}
