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

import org.apache.spark.h2o.converters.WriteConverterCtx
import org.apache.spark.h2o.converters.WriteConverterCtxUtils.UploadPlan
import org.apache.spark.h2o.utils.SupportedTypes._
import org.apache.spark.h2o.utils.{NodeDesc, ReflectionUtils}
import org.apache.spark.sql.types._
import water.{ExternalFrameConfirmationException, ExternalFrameUtils, ExternalFrameWriterClient}

class ExternalWriteConverterCtx(nodeDesc: NodeDesc, totalNumOfRows: Int, writeTimeout: Int) extends WriteConverterCtx {

  val socketChannel = ExternalFrameUtils.getConnection(nodeDesc.hostname, nodeDesc.port)
  val externalFrameWriter = new ExternalFrameWriterClient(socketChannel)

  /**
    * This method closes the communication after the chunks have been closed
    * @throws ExternalFrameConfirmationException in case of confirmation failure.
    *         This has also effect of Spark stopping the current job and
    *         rescheduling it
    */
  override def closeChunks(): Unit = {
    try{
      externalFrameWriter.waitUntilAllWritten(writeTimeout)
    } finally {
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
  // Here we should call externalFrameWriter.sendDate - however such method does not exist
  // Hence going through the same path as sending Timestamp long value.
  override def put(colIdx: Int, data: java.sql.Date) = externalFrameWriter.sendLong(data.getTime)
  override def put(colIdx: Int, data: String) = externalFrameWriter.sendString(data)
  override def putNA(columnNum: Int) = externalFrameWriter.sendNA()

  override def numOfRows(): Int = totalNumOfRows
}


object ExternalWriteConverterCtx extends ExternalBackendUtils {

  import scala.language.postfixOps
  import scala.reflect.runtime.universe._

  def scheduleUpload(numPartitions: Int): UploadPlan = {
    val nodes = cloudMembers
    val uploadPlan = (0 until numPartitions).zip(Stream.continually(nodes).flatten).toMap
    uploadPlan
  }

  // In external cluster we need to use just the basic types allowed for the conversion. Supported types has
  // associated java classes but that's actually not the type as which the data are transferred. The following methods
  // overrides this behaviour so the correct internal type for transfer is returned
  def internalJavaClassOf[T](implicit ttag: TypeTag[T]) = ReflectionUtils.javaClassOf[T]

  def internalJavaClassOf(supportedType: SupportedType) = {
    supportedType match {
      case Date => Long.javaClass
      case _ => supportedType.javaClass
    }
  }
  def internalJavaClassOf(dt: DataType) : Class[_] = {
    dt match {
      case n if n.isInstanceOf[DecimalType] & n.getClass.getSuperclass != classOf[DecimalType] => Double.javaClass
      case _ : DateType => Long.javaClass
      case _ : DataType => ReflectionUtils.supportedTypeOf(dt).javaClass
    }
  }
}
