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
import org.apache.spark.h2o.backends.internal.InternalBackendConf
import org.apache.spark.h2o.utils.{SharedH2OTestContext, TestFrameUtils}
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class H2OContextConversionOnSubsetExecutors extends FunSuite with SharedH2OTestContext {

  override def createSparkContext: SparkContext = new SparkContext(
    "local-cluster[3,1,1024]",
    this.getClass.getName,
    conf = defaultSparkConf.set(InternalBackendConf.PROP_CLUSTER_SIZE._1, "1"))

  test("asH2OFrame conversion on subset of executors") {
    assert(hc.getH2ONodes().length == 1)
    val rdd = sc.parallelize(1 to 1000, 100).map(v => DoubleHolder(Some(v)))
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

  test("asDataFrame conversion on subset of executors") {
    assert(hc.getH2ONodes().length == 1)
    val originalRdd = sc.parallelize(1 to 1000, 100).map(v => DoubleHolder(Some(v)))
    val hf = hc.asH2OFrame(originalRdd)

    val convertedDf = hc.asDataFrame(hf)
    import spark.implicits._
    TestFrameUtils.assertDataFramesAreIdentical(originalRdd.toDF, convertedDf.toDF())
  }

  test("asRDD conversion on subset of executors") {
    assert(hc.getH2ONodes().length == 1)
    import spark.implicits._
    val originalRdd = sc.parallelize(1 to 1000, 100).map(v => DoubleHolder(Some(v)))
    val hf = hc.asH2OFrame(originalRdd)

    val convertedRdd = hc.asRDD[DoubleHolder](hf)

    TestFrameUtils.assertDataFramesAreIdentical(originalRdd.toDF, convertedRdd.toDF())
  }
}
