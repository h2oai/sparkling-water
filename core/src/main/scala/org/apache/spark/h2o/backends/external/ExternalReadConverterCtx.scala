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


import org.apache.spark.h2o.converters.ReadConverterCtx
import org.apache.spark.h2o.utils.NodeDesc
import water.ExternalFrameReaderClient

/**
  *
  * @param keyName  key name of frame to query data from
  * @param chunkIdx chunk index
  * @param nodeDesc the h2o node which has data for chunk with the chunkIdx
  */
class ExternalReadConverterCtx(override val keyName: String, override val chunkIdx: Int,
                               val nodeDesc: NodeDesc, expectedTypes: Array[Byte], selectedColumnIndices: Array[Int],
                               val driverTimeStamp: Short)
  extends ReadConverterCtx {
  override type DataSource = ExternalFrameReaderClient

  private val externalFrameReader = ExternalFrameReaderClient.create(
    nodeDesc.hostname, nodeDesc.port, driverTimeStamp,
    keyName, chunkIdx, selectedColumnIndices, expectedTypes)

  override def numRows: Int = externalFrameReader.getNumRows

  override def returnOption[T](read: DataSource => T)(columnNum: Int): Option[T] = {
    Option(read(externalFrameReader)).filter(_ => !externalFrameReader.isLastNA)
  }

  override def returnSimple[T](ifMissing: String => T, read: DataSource => T)(columnNum: Int): T = {
    val value = read(externalFrameReader)
    if (externalFrameReader.isLastNA) ifMissing(s"Row $rowIdx column $columnNum") else value
  }

  override protected def booleanAt(source: DataSource): Boolean = source.readBoolean()

  override protected def byteAt(source: DataSource): Byte = source.readByte()

  override protected def shortAt(source: DataSource): Short = source.readShort()

  override protected def intAt(source: DataSource): Int = source.readInt()

  override protected def longAt(source: DataSource): Long = source.readLong()

  override protected def floatAt(source: DataSource): Float = source.readFloat()

  override protected def doubleAt(source: DataSource): Double = source.readDouble()

  override protected def string(source: DataSource) = source.readString()

  override def hasNext: Boolean = {
    val isNext = super.hasNext
    if (!isNext) {
      externalFrameReader.close()
    }
    isNext
  }
}
