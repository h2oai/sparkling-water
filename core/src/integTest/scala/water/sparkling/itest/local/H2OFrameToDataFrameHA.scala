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
import org.apache.spark.h2o.DoubleHolder
import org.apache.spark.h2o.utils.{SharedH2OTestContext, TestFrameUtils}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

/**
 * Test High Concurrency during conversion master
 * is set to local-cluster in this test suite.
 */
@RunWith(classOf[JUnitRunner])
class H2OFrameToDataFrameHA extends FunSuite with SharedH2OTestContext {

  override def createSparkContext: SparkContext = new SparkContext("local-cluster[2,1,1024]", this.getClass.getName, conf = defaultSparkConf)

  test("Task killed but frame still converted successfully") {
    val rdd = sc.parallelize(1 to 1000, 100).map(v => DoubleHolder(Some(v))).map { d =>
      import org.apache.spark.TaskContext
      val tc = TaskContext.get()
      if (tc.attemptNumber == 0) {
        throw new Exception("Failing first attempt")
      } else {
        d
      }
    }

    val h2oFrame = hc.asH2OFrame(rdd)

    TestFrameUtils.assertBasicInvariants(rdd, h2oFrame, (rowIdx, vec) => {
      val nextRowIdx = rowIdx + 1
      val value = vec.at(rowIdx) // value stored at rowIdx-th
      // Using == since int should be mapped strictly to doubles
      assert(nextRowIdx == value, "The H2OFrame values should match row numbers+1")
    })

    // Clean up
    h2oFrame.delete()
  }
}
