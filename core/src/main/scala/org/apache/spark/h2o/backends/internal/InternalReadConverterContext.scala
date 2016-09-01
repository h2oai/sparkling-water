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
import water.fvec.{Chunk, Frame}
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

  private def get[T](columnNum: Int, read: Chunk => T): Option[T] =
    if (isNA(columnNum)) {
      None
    } else {
      Option(read(chks(columnNum)))
    }

  override def getLong(columnNum: Int): Option[Long] =  get(columnNum, _.at8(rowIdx))
  override def getDouble(columnNum: Int): Option[Double] = get(columnNum, _.atd(rowIdx))

  // TODO(vlad): try to move out all this logic to prepared extractors
  override def getString(columnNum: Int): Option[String] = get(columnNum, chunk =>
    {
    val chunk = chks(columnNum)
    val vector = chunk.vec()
    if (vector.isCategorical) {
      val str = vector.domain()(chunk.at8(rowIdx).toInt)
      str
    } else if (vector.isString) {
      val valStr = new BufferedString()
      chunk.atStr(valStr, rowIdx) // TODO improve this.
      valStr.toString
    } else if (vector.isUUID) {
      val uuid = new UUID(chunk.at16h(rowIdx), chunk.at16l(rowIdx))
      uuid.toString
    } else {
      assert(assertion = false, s"Should never be here, type is ${vector.get_type_str()}")
      // TODO(vlad): this is temporarily here, to provide None is string is missing
      null
    }
    })

  private def underlyingFrame = DKV.get(Key.make(keyName)).get.asInstanceOf[Frame]

  override def numRows: Int = nrows
}
