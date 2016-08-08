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
package org.apache.spark.h2o.utils

import java.util.UUID

import org.apache.spark.SparkContext
import org.apache.spark.h2o.{H2OConf, H2OContext, Holder}
import org.apache.spark.sql.SQLContext
import org.scalatest.Suite
import water.fvec.{Chunk, FrameUtils, NewChunk, Vec}
import water.parser.BufferedString

import scala.reflect.ClassTag

/**
  * Helper trait to simplify initialization and termination of Spark/H2O contexts.
  *
  */
trait SharedSparkTestContext extends SparkTestContext with ExternalClusterModeTestHelper { self: Suite =>

  def createSparkContext:SparkContext

  def createH2OContext(sc: SparkContext, conf: H2OConf): H2OContext = {
    H2OContext.getOrCreate(sc, conf)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = createSparkContext
    sqlc = SQLContext.getOrCreate(sc)
    if(testsInExternalMode(sc.getConf)){
      startCloud(2, sc.getConf)
    }
    hc = createH2OContext(sc, new H2OConf(sc).setNumOfExternalH2ONodes(2))
  }


  override def afterAll(): Unit = {
    if(testsInExternalMode(sc.getConf)){
      stopCloud()
    }
    resetContext()
    super.afterAll()
  }

  def buildChunks[T: ClassTag](fname: String, data: Array[T], cidx: Integer, h2oType: Array[Byte]): Chunk = {
    val nchunks: Array[NewChunk] = FrameUtils.createNewChunks(fname, h2oType, cidx)

    val chunk: NewChunk = nchunks(0)
    data.foreach {
      case u: UUID               => chunk.addUUID(
        u.getLeastSignificantBits,
        u.getMostSignificantBits)
      case s: String             => chunk.addStr(new BufferedString(s))
      case b: Byte               => chunk.addNum(b)
      case s: Short              => chunk.addNum(s)
      case c: Integer if h2oType(0) == Vec.T_CAT => chunk.addCategorical(c)
      case i: Integer if h2oType(0) != Vec.T_CAT => chunk.addNum(i.toDouble)
      case l: Long               => chunk.addNum(l)
      case d: Double             => chunk.addNum(d)
      case x                     =>
        throw new IllegalArgumentException(s"Failed to figure out what is it: $x")
    }
    FrameUtils.closeNewChunks(nchunks)
    chunk
  }
}

class TestMemory[T] extends scala.collection.mutable.HashSet[T] with scala.collection.mutable.SynchronizedSet[T] {
  def put(xh: Holder[T]): Unit = xh.result foreach put

  def put(x: T): Unit = {
    if (this contains x) {
      throw new IllegalStateException(s"Duplicate element $x in test memory")
    }
    add(x)
  }
}

