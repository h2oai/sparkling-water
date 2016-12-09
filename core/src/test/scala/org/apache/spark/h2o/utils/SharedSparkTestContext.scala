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
import java.util.concurrent.ConcurrentHashMap

import org.apache.spark.SparkContext
import org.apache.spark.h2o.{BackendIndependentTestHelper, H2OConf, H2OContext, Holder}
import org.scalatest.Suite
import water.fvec._
import water.{DKV, Key}
import water.parser.BufferedString

import scala.reflect.ClassTag

/**
  * Helper trait to simplify initialization and termination of Spark/H2O contexts.
  *
  */
trait SharedSparkTestContext extends SparkTestContext with BackendIndependentTestHelper {
  self: Suite =>

  def createSparkContext: SparkContext

  def createH2OContext(sc: SparkContext, conf: H2OConf): H2OContext = {
    H2OContext.getOrCreate(sc, conf)
  }

  override def beforeAll(): Unit = {
    super.beforeAll()
    sc = createSparkContext
    hc = createH2OContext(sc, 2)
  }


  override def afterAll(): Unit = {
    stopCloudIfExternal(sc)
    resetContext()
    super.afterAll()
  }

  def makeH2OFrame[T: ClassTag](fname: String, colNames: Array[String], chunkLayout: Array[Long],
                                data: Array[Array[T]], h2oType: Byte, colDomains: Array[Array[String]] = null): H2OFrame = {
    makeH2OFrame2(fname, colNames, chunkLayout, data.map(_.map(value => Array(value))), Array(h2oType), colDomains)
  }

  def makeH2OFrame2[T: ClassTag](fname: String, colNames: Array[String], chunkLayout: Array[Long],
                                 data: Array[Array[Array[T]]], h2oTypes: Array[Byte], colDomains: Array[Array[String]] = null): H2OFrame = {
    var f: Frame = new Frame(Key.make[Frame](fname))
    FrameUtils.preparePartialFrame(f, colNames)
    f.update()

    for (i <- chunkLayout.indices) {
      buildChunks(fname, data(i), i, h2oTypes)
    }

    f = DKV.get(fname).get()

    FrameUtils.finalizePartialFrame(f, chunkLayout, colDomains, h2oTypes)

    new H2OFrame(f)
  }

  def buildChunks[T: ClassTag](fname: String, data: Array[Array[T]], cidx: Integer, h2oType: Array[Byte]): Array[_ <: Chunk] = {
    val nchunks: Array[NewChunk] = FrameUtils.createNewChunks(fname, h2oType, cidx)

    data.foreach { values =>
      values.indices.foreach { idx =>
        val chunk: NewChunk = nchunks(idx)
        values(idx) match {
          case null                                  => chunk.addNA()
          case u: UUID                               => chunk.addUUID(u.getLeastSignificantBits, u.getMostSignificantBits)
          case s: String                             => chunk.addStr(new BufferedString(s))
          case b: Byte                               => chunk.addNum(b)
          case s: Short                              => chunk.addNum(s)
          case c: Integer if h2oType(0) == Vec.T_CAT => chunk.addCategorical(c)
          case i: Integer if h2oType(0) != Vec.T_CAT => chunk.addNum(i.toDouble)
          case l: Long                               => chunk.addNum(l)
          case d: Double                             => chunk.addNum(d)
          case x =>
            throw new IllegalArgumentException(s"Failed to figure out what is it: $x")
        }
      }
    }
    FrameUtils.closeNewChunks(nchunks)
    nchunks
  }
}

class TestMemory[T] extends ConcurrentHashMap[T, Unit] {
  def put(xh: Holder[T]): Unit = xh.result foreach put

  def put(x: T): Unit = {
    if (this contains x) {
      throw new IllegalStateException(s"Duplicate element $x in test memory")
    }
    put(x, ())
  }
}

