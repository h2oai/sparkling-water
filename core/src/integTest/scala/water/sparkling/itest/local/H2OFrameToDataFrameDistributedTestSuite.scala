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
import org.apache.spark.h2o.utils.SharedH2OTestContext
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
 * Test conversion from H2O Frame to Spark Data Frame in a distributed environment. That is why the master
 * is set to local-cluster in this test suite.
 */
@RunWith(classOf[JUnitRunner])
class H2OFrameToDataFrameDistributedTestSuite extends FunSuite with SharedH2OTestContext {

  override def createSparkContext: SparkContext = new SparkContext("local-cluster[2,1,1024]", this.getClass.getName, conf = defaultSparkConf)

  test("Convert H2OFrame to DataFrame when H2OFrame was changed in DKV") {

    val rdd = sc.parallelize(1 to 100, 2)
    val h2oFrame = hc.asH2OFrame(rdd)
    assert(h2oFrame.anyVec().nChunks() == 2)
    val updatedFrame = h2oFrame.add(h2oFrame)

    val convertedDf = hc.asDataFrame(updatedFrame)
    convertedDf.collect() // trigger materialization

    assert(convertedDf.count() == h2oFrame.numRows())
    assert(convertedDf.columns.length == h2oFrame.names().length)
  }

}
