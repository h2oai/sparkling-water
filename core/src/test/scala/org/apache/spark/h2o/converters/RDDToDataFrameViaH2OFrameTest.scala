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

package org.apache.spark.h2o.converters

import org.apache.spark.SparkContext
import org.apache.spark.h2o.IntHolder
import org.apache.spark.h2o.utils.SharedSparkTestContext
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import water.fvec.H2OFrame

/**
  * Test conversion from RDD to DataFrame via H2OFrame
  */
class RDDToDataFrameViaH2OFrameTest extends FunSuite with SharedSparkTestContext with BeforeAndAfterAll  {

  override def createSparkContext: SparkContext = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)

  test("Convert RDD to H2OFrame and back to DataFrame") {
    val h2oContext = hc
    import h2oContext.implicits._

    val rdd = sc.parallelize(1 to 10000, 1000).map(i => IntHolder(Some(i)))
    val h2oFrame: H2OFrame = rdd

    val dataFrame = hc.asDataFrame(h2oFrame)

    assert (rdd.count == h2oFrame.numRows())
    assert (rdd.count == dataFrame.count)
  }
}
