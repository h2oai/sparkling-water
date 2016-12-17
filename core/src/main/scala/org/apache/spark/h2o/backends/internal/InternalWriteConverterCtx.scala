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

import java.sql.Timestamp

import org.apache.spark.h2o.converters.WriteConverterCtx
import water.fvec.{FrameUtils, NewChunk}

class InternalWriteConverterCtx extends WriteConverterCtx {

  private var chunks: Array[NewChunk] = _
  override def createChunks(keyName: String, vecTypes: Array[Byte], chunkId: Int): Unit = {
   chunks = FrameUtils.createNewChunks(keyName, vecTypes, chunkId)
  }

  override def closeChunks(): Unit = {
    FrameUtils.closeNewChunks(chunks)
  }

  override def put(columnNum: Int, data: Boolean): Unit =  chunks(columnNum).addNum(if (data) 1 else 0)
  override def put(columnNum: Int, data: Byte): Unit = chunks(columnNum).addNum(data)
  override def put(columnNum: Int, data: Char): Unit = chunks(columnNum).addNum(data)
  override def put(columnNum: Int, data: Short): Unit = chunks(columnNum).addNum(data)
  override def put(columnNum: Int, data: Int): Unit = chunks(columnNum).addNum(data)
  override def put(columnNum: Int, data: Long): Unit = chunks(columnNum).addNum(data)
  override def put(columnNum: Int, data: Float): Unit = chunks(columnNum).addNum(data)
  override def put(columnNum: Int, data: Double): Unit = chunks(columnNum).addNum(data)
  override def put(columnNum: Int, data: Timestamp): Unit = chunks(columnNum).addNum(data.getTime)
  override def put(columnNum: Int, data: String): Unit = chunks(columnNum).addStr(data)
  override def putNA(columnNum: Int): Unit = chunks(columnNum).addNA()

  override def numOfRows(): Int = chunks(0).len()
}
