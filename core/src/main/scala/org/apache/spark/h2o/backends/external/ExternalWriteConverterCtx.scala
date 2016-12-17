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

import org.apache.spark.h2o._
import org.apache.spark.h2o.converters.WriteConverterCtx
import org.apache.spark.h2o.converters.WriteConverterCtxUtils.UploadPlan
import org.apache.spark.h2o.utils.NodeDesc
import water.{ExternalFrameUtils, ExternalFrameWriterClient}

class ExternalWriteConverterCtx(nodeDesc: NodeDesc, totalNumOfRows: Int) extends WriteConverterCtx {

  val socketChannel = ExternalFrameUtils.getConnection(nodeDesc.hostname, nodeDesc.port)
  val externalFrameWriter = new ExternalFrameWriterClient(socketChannel)

  /**
    * This method closes the communication after the chunks have been closed
    */
  override def closeChunks(): Unit = {
    try{
      externalFrameWriter.waitUntilAllWritten()
    }finally {
      socketChannel.close()
    }
  }

  /**
    * Initialize the communication before the chunks are created
    */
  override def createChunks(keystr: String, expectedTypes: Array[Byte], chunkId: Int): Unit = {
    externalFrameWriter.createChunks(keystr, expectedTypes, chunkId, totalNumOfRows)
  }

  override def put(colIdx: Int, data: Boolean) = externalFrameWriter.sendBoolean(data)
  override def put(colIdx: Int, data: Byte) = externalFrameWriter.sendByte(data)
  override def put(colIdx: Int, data: Char) = externalFrameWriter.sendChar(data)
  override def put(colIdx: Int, data: Short) = externalFrameWriter.sendShort(data)
  override def put(colIdx: Int, data: Int) = externalFrameWriter.sendInt(data)
  override def put(colIdx: Int, data: Long) = externalFrameWriter.sendLong(data)
  override def put(colIdx: Int, data: Float) = externalFrameWriter.sendFloat(data)
  override def put(colIdx: Int, data: Double) = externalFrameWriter.sendDouble(data)
  override def put(colIdx: Int, data: java.sql.Timestamp) = externalFrameWriter.sendTimestamp(data)
  override def put(colIdx: Int, data: String) = externalFrameWriter.sendString(data)
  override def putNA(columnNum: Int) = externalFrameWriter.sendNA()

  override def numOfRows(): Int = totalNumOfRows
}


object ExternalWriteConverterCtx extends ExternalBackendUtils {

  def scheduleUpload(numPartitions: Int): UploadPlan = {
    val nodes = cloudMembers
    val uploadPlan = (0 until numPartitions).zip(Stream.continually(nodes).flatten).toMap
    uploadPlan
  }
}
