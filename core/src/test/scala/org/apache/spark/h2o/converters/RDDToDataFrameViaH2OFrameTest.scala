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
import org.apache.spark.h2o.utils.SharedH2OTestContext
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import water.{Key, MRTask}
import water.fvec.{Chunk, H2OFrame, NewChunk, Vec}

/**
  * Test conversion from RDD to DataFrame via H2OFrame
  */
class RDDToDataFrameViaH2OFrameTest extends FunSuite with SharedH2OTestContext with BeforeAndAfterAll  {

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

  // @formatter:off
  test("SW-559: Convert RDD to H2OFrame with 0-length chunks") {
    // Generate RDD which contains empty partitions
    val rdd = sc.parallelize(1 to 10, 10).map(i => IntHolder(Some(i))).filter(x => x.result.get < 5)
    assert(rdd.partitions.length == 10, "Number of partitions after filtering should be 10")

    // The frame contains number of chunks == number of RDD partitions, but some of
    val hf: H2OFrame = hc.asH2OFrame(rdd)
    val espcBeforeMr = hf.anyVec().espc()
    // Now touch the frame with MRTask which produces a new vector,
    // which is closed by AppendableVec.close() call (see Frame#closeFrame() method)
    val zeroHf = new ZeroMR().doAll(Array(Vec.T_NUM), hf).outputFrame(Array("X"), null)
    val espcAfterMr = zeroHf.anyVec().espc()
    // The ESPC of frame vectors before and after invocation of MRTask should match
    assert(espcBeforeMr === espcAfterMr, "The ESPC of frame vectors before and after invocation of MRTask should match")
  }

  class ZeroMR extends MRTask[ZeroMR] {
    override def map(c: Chunk, nc: NewChunk): Unit = {
      for (i <- 0 until c.len()) {
        nc.addNum(0.0)
      }
    }
  }
}
