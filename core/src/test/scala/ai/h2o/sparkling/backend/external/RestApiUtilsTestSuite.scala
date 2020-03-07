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

package ai.h2o.sparkling.backend.external

import ai.h2o.sparkling.extensions.rest.api.Paths
import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SharedH2OTestContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RestApiUtilsTestSuite extends FunSuite with SharedH2OTestContext {
  override def createSparkContext: SparkContext = new SparkContext("local[*]", getClass.getSimpleName, defaultSparkConf)

  test("Error message from unsuccessful call contains information from the server") {
    val conf = hc.getConf
    val endpoint = RestApiUtils.getClusterEndpoint(conf)

    val caught = intercept[RestApiCommunicationException](RestApiUtils.update(endpoint, Paths.CHUNK, conf))

    assert(caught.getMessage.contains("Cannot find value for the parameter 'frame_name'"))
  }
}
