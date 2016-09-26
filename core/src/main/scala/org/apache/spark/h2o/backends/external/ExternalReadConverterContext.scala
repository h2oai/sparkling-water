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

package org.apache.spark.h2o.backends.external

import org.apache.spark.h2o.converters.ReadConverterContext
import org.apache.spark.h2o.utils.NodeDesc
import water.AutoBufferUtils._
import water.{AutoBuffer, AutoBufferUtils, ExternalFrameHandler}
/**
  *
  * @param keyName key name of frame to query data from
  * @param chunkIdx chunk index
  * @param nodeDesc the h2o node which has data for chunk with the chunkIdx
  */
class ExternalReadConverterContext(override val keyName: String, override val chunkIdx: Int,
                                    val nodeDesc: NodeDesc, expectedTypes: Array[Byte], selectedColumnIndices: Array[Int])
  extends ExternalBackendUtils with ReadConverterContext {

  private val socketChannel = ConnectionToH2OHelper.getOrCreateConnection(nodeDesc)
  private val inputAb = createInputAutoBuffer()
  private val numOfRows: Int =  AutoBufferUtils.getInt(inputAb)
  private def createInputAutoBuffer(): AutoBuffer = {
    val ab = new AutoBuffer()
    ab.put1(ExternalFrameHandler.INIT_BYTE)
    ab.putInt(ExternalFrameHandler.DOWNLOAD_FRAME)
    ab.putStr(keyName)
    ab.putA1(expectedTypes)
    ab.putInt(chunkIdx)
    ab.putA4(selectedColumnIndices)
    writeToChannel(ab, socketChannel)
    val inAb = AutoBufferUtils.create(socketChannel)
    inAb
  }

  override def numRows: Int = numOfRows

  private def get[T](columnNum: Int, read: AutoBuffer => T): Option[T] =
    if (isNA(columnNum)) {
      None
    } else {
      Option(read(inputAb))
    }

  /**
    * method isNA has to be called before any other method which gets data:
    * getDouble, getLong, getString, getShort, getByte, getFloat, getUTF8String, getBoolean, getTimestamp
    *
    * It should be always called like : isNA(..);get..(..); isNA(..);get..(..);....
    */
  override def isNA(columnNum: Int): Boolean = AutoBufferUtils.getInt(inputAb) == ExternalFrameHandler.IS_NA

  override def getDouble(columnNum: Int): Option[Double] = get(columnNum, _.get8d())

  override def getLong(columnNum: Int): Option[Long] = get(columnNum, _.get8())

  override def getString(columnNum: Int): Option[String] = get(columnNum, _.getStr())

  override def hasNext: Boolean = {
    val isNext = super.hasNext
    if(!isNext){
      // confirm that all has been done before proceeding with the computation
      assert(AutoBufferUtils.getInt(inputAb)==ExternalFrameHandler.CONFIRM_READING_DONE)

      // put back socket channel to pool of opened connections after we get the last element
      ConnectionToH2OHelper.putAvailableConnection(nodeDesc, socketChannel)
    }
    isNext
  }
}
