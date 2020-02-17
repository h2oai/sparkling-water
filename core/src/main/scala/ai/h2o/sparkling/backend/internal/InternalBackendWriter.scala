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

package ai.h2o.sparkling.backend.internal

import java.sql.{Date, Timestamp}

import ai.h2o.sparkling.backend.shared.Writer
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import water.fvec.FrameUtils

private[sparkling] class InternalBackendWriter(frameName: String,
                                               numRows: Int,
                                               expectedTypes: Array[Byte],
                                               chunkId: Int,
                                               sparse: Array[Boolean],
                                               vecStartSize: Map[Int, Int]) extends Writer {
  private val chunks = FrameUtils.createNewChunks(frameName, expectedTypes, chunkId, sparse)
  private val sparseVectorPts = collection.mutable.Map(vecStartSize.mapValues(size => new Array[Int](size)).toSeq: _*)
  private val sparseVectorInUse = collection.mutable.Map(vecStartSize.mapValues(_ => false).toSeq: _*)

  private var rowIdx: Int = _

  override def put(columnNum: Int, data: Boolean): Unit = chunks(columnNum).addNum(if (data) 1 else 0)

  override def put(columnNum: Int, data: Byte): Unit = chunks(columnNum).addNum(data)

  override def put(columnNum: Int, data: Char): Unit = chunks(columnNum).addNum(data)

  override def put(columnNum: Int, data: Short): Unit = chunks(columnNum).addNum(data)

  override def put(columnNum: Int, data: Int): Unit = chunks(columnNum).addNum(data)

  override def put(columnNum: Int, data: Long): Unit = chunks(columnNum).addNum(data)

  override def put(columnNum: Int, data: Float): Unit = chunks(columnNum).addNum(data)

  override def put(columnNum: Int, data: Double): Unit = chunks(columnNum).addNum(data)

  override def put(columnNum: Int, data: Timestamp): Unit = chunks(columnNum).addNum(data.getTime)

  override def put(columnNum: Int, data: Date): Unit = chunks(columnNum).addNum(data.getTime)

  override def put(columnNum: Int, data: String): Unit = chunks(columnNum).addStr(data)

  override def putNA(columnNum: Int, sparkIdx: Int): Unit = chunks(columnNum).addNA()

  override def putSparseVector(startIdx: Int, vector: SparseVector, maxVecSize: Int): Unit = {
    sparseVectorInUse(startIdx) = true
    val sparseVectorPt = sparseVectorPts(startIdx)
    var i = 0
    while (i < vector.indices.length) {
      val idx = vector.indices(i)
      val value = vector.values(i)
      val zeros = rowIdx - sparseVectorPt(idx)
      if (zeros > 0) {
        chunks(startIdx + idx).addZeros(zeros)
      }
      put(startIdx + idx, value)
      sparseVectorPt(idx) = rowIdx + 1
      i += 1
    }
  }

  override def startRow(rowIdx: Int): Unit = {
    this.rowIdx = rowIdx
  }

  override def putDenseVector(startIdx: Int, vector: DenseVector, maxVecSize: Int): Unit = {
    (0 until vector.size).foreach { idx => put(startIdx + idx, vector(idx)) }

    (vector.size until maxVecSize).foreach(idx => put(startIdx + idx, 0.0))
  }

  override def close(): Unit = {
    sparseVectorPts.foreach { case (startIdx, pts) =>
      if (sparseVectorInUse(startIdx)) {
        var i = 0
        while (i < pts.length) {
          val lastRowIdx = pts(i)
          if (lastRowIdx < numRows) {
            chunks(startIdx + i).addZeros(numRows - lastRowIdx)
          }
          i += 1
        }
      }
    }
    FrameUtils.closeNewChunks(chunks)
  }
}
