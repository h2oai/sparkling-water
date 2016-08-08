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
import org.apache.spark.h2o.converters.WriteConverterContext
import org.apache.spark.h2o.utils.NodeDesc
import water.AutoBufferUtils._
import water.{AutoBuffer, AutoBufferUtils, ExternalFrameHandler}

class ExternalWriteConverterContext(nodeDesc: NodeDesc) extends ExternalBackendUtils with WriteConverterContext {

  val socketChannel = ConnectionToH2OHelper.getOrCreateConnection(nodeDesc)
  var rowCounter: Long = 0
  private val ab = new AutoBuffer().flipForReading() // using default constructor AutoBuffer is created with
  // private property _read set to false, in order to satisfy call clearForWriting it has to be set to true
  // which does the call of flipForReading method

  override def numOfRows(): Long = rowCounter
  /**
    * This method closes the communication after the chunks have been closed
    */
  override def closeChunks(): Unit = {
    AutoBufferUtils.clearForWriting(ab)
    ab.putInt(ExternalFrameHandler.CLOSE_NEW_CHUNK)
    writeToChannel(ab, socketChannel)
    val confirmAb = AutoBufferUtils.create(socketChannel)
    // this needs to be here because confirmAb forces this code to wait
    // for all the work to be done on the recipient side. The assert is just additional, not so important check
    assert(AutoBufferUtils.getInt(confirmAb)==ExternalFrameHandler.CONFIRM_WRITING_DONE)
    ConnectionToH2OHelper.putAvailableConnection(nodeDesc, socketChannel)
  }

  /**
    * Initialize the communication before the chunks are created
    */
  override def createChunks(keystr: String, vecTypes: Array[Byte], chunkId: Int): Unit = {
    AutoBufferUtils.clearForWriting(ab)
    ab.put1(ExternalFrameHandler.INIT_BYTE)
    ab.putInt(ExternalFrameHandler.CREATE_FRAME)
    ab.putInt(ExternalFrameHandler.CREATE_NEW_CHUNK)
    ab.putStr(keystr)
    ab.putA1(vecTypes)
    ab.putInt(chunkId)
    writeToChannel(ab, socketChannel)
  }


  override def put(columnNum: Int, n: Number) = {
    AutoBufferUtils.clearForWriting(ab)
    ab.putInt(ExternalFrameHandler.ADD_TO_CHUNK)
    ab.putInt(ExternalFrameHandler.TYPE_NUM)
    ab.putInt(columnNum)
    ab.put8d(n.doubleValue())
    writeToChannel(ab, socketChannel)
  }


  override def put(columnNum: Int, n: Boolean) = {
    AutoBufferUtils.clearForWriting(ab)
    ab.putInt(ExternalFrameHandler.ADD_TO_CHUNK)
    ab.putInt(ExternalFrameHandler.TYPE_NUM)
    ab.putInt(columnNum)
    ab.put8d(if (n) 1 else 0)
    writeToChannel(ab, socketChannel)
  }

  override def put(columnNum: Int, n: java.sql.Timestamp) = {
    AutoBufferUtils.clearForWriting(ab)
    ab.putInt(ExternalFrameHandler.ADD_TO_CHUNK)
    ab.putInt(ExternalFrameHandler.TYPE_NUM)
    ab.putInt(columnNum)
    ab.put8d(n.getTime())
    writeToChannel(ab, socketChannel)
  }

  override def put(columnNum: Int, n: String) = {
    AutoBufferUtils.clearForWriting(ab)
    ab.putInt(ExternalFrameHandler.ADD_TO_CHUNK)
    ab.putInt(ExternalFrameHandler.TYPE_STR)
    ab.putInt(columnNum)
    ab.putStr(n)
    writeToChannel(ab, socketChannel)
  }

  override def putNA(columnNum: Int) = {
    AutoBufferUtils.clearForWriting(ab)
    ab.putInt(ExternalFrameHandler.ADD_TO_CHUNK)
    ab.putInt(ExternalFrameHandler.TYPE_NA)
    ab.putInt(columnNum)
    writeToChannel(ab, socketChannel)
  }

  override def increaseRowCounter(): Unit = rowCounter = rowCounter + 1
}


object ExternalWriteConverterContext extends ExternalBackendUtils{

  def scheduleUpload[T](rdd: RDD[T]): (RDD[T], Map[Int, NodeDesc]) = {
    val nodes = cloudMembers
    val preparedRDD =  if (rdd.getNumPartitions < nodes.length) {
      // repartition to get same amount of partitions as is number of h2o nodes
      rdd.repartition(nodes.length)
    } else{
      // in case number of partitions is equal or higher than number of H2O nodes return original rdd
      // We are not repartitioning when number of partitions is bigger than number of h2o nodes since the data
      // in one partition could then become too big for one h2o node, we rather create multiple chunks at one h2o node
      rdd
    }

    // numPartitions is always >= than nodes.length at this step
    val partitionIdxs = 0 until preparedRDD.getNumPartitions
    val uploadPlan = partitionIdxs.zip(Stream.continually(nodes).flatten).toMap

    (preparedRDD, uploadPlan)
  }
}
