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

import org.apache.spark.h2o.converters.WriteConverterContext
import water.fvec.{FrameUtils, NewChunk}

class InternalWriteConverterContext extends WriteConverterContext{
  var nchnks: Array[NewChunk] = _
  override def createChunks(keyName: String, vecTypes: Array[Byte], chunkId: Int): Unit = {
   nchnks = FrameUtils.createNewChunks(keyName, vecTypes, chunkId)
  }

  override def closeChunks(): Unit = {
    FrameUtils.closeNewChunks(nchnks)
  }

  override def put(columnNum: Int, n: Number): Unit = nchnks(columnNum).addNum(n.doubleValue())

  override def put(columnNum: Int, n: Boolean): Unit =  nchnks(columnNum).addNum(if (n) 1 else 0)

  override def put(columnNum: Int, n: Timestamp): Unit = nchnks(columnNum).addNum(n.getTime)

  override def put(columnNum: Int, str: String): Unit = nchnks(columnNum).addStr(str)

  override def putNA(columnNum: Int): Unit = nchnks(columnNum).addNA()

  override def increaseRowCounter(): Unit = { }// empty on purpose, we can get number or rows without counter

  override def numOfRows: Long = nchnks(0).len()
}
