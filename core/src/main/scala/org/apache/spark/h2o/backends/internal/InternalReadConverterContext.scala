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

package org.apache.spark.h2o.backends.internal

import java.util.UUID

import org.apache.spark.h2o.converters.ReadConverterContext
import water.fvec.{Chunk, Frame, Vec}
import water.parser.BufferedString
import water.{DKV, Key}

class InternalReadConverterContext(override val keyName: String, override val chunkIdx: Int) extends ReadConverterContext{

  /** Lazily fetched H2OFrame from K/V store */
  private lazy val fr: Frame = underlyingFrame

  /** Chunks for this partition */
  private lazy val chks: Array[Chunk] = water.fvec.FrameUtils.getChunks(fr, chunkIdx)

  /** Number of rows in this partition */
  private lazy val nrows = chks(0)._len

  override def isNA(columnNum: Int): Boolean = chks(columnNum).isNA(rowIdx)

  override def getLong(columnNum: Int): Long =  chks(columnNum).at8(rowIdx)

  override def getDouble(columnNum: Int): Double =  chks(columnNum).atd(rowIdx)

  override def getString(columnNum: Int): String = {
    val chunk = chks(columnNum)
    val vec: Vec = chunk.vec()
    if (vec.isCategorical) {
      val str = vec.domain()(chunk.at8(rowIdx).asInstanceOf[Int])
      str
    } else if (vec.isString) {
      val valStr = new BufferedString()
      chunk.atStr(valStr, rowIdx) // TODO improve this.
      valStr.toString
    } else if (vec.isUUID) {
      val uuid = new UUID(chunk.at16h(rowIdx), chunk.at16l(rowIdx))
      uuid.toString
    } else{
      assert(assertion = false, "Should never be here")
      null
    }
  }

  private def underlyingFrame = DKV.get(Key.make(keyName)).get.asInstanceOf[Frame]

  override def numRows: Int = nrows
}
