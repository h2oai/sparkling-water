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

import ai.h2o.sparkling.backend.shared.WriteConverterCtx
import org.apache.spark.h2o.Frame
import org.apache.spark.ml.linalg.{DenseVector, SparseVector}
import water.fvec.FrameUtils._
import water.fvec.{Chunk, FrameUtils, NewChunk}
import water.util.Log
import water.{DKV, Key, MRTask, _}

import scala.collection.mutable

class InternalWriteConverterCtx extends WriteConverterCtx {

  private var chunks: Array[NewChunk] = _

  private var sparseVectorPts: collection.mutable.Map[Int, Array[Int]] = _
  private var sparseVectorInUse: collection.mutable.Map[Int, Boolean] = _

  private var rowIdx: Int = _

  override def initFrame(key: String, columns: Array[String]): Unit = {
    val fr = new water.fvec.Frame(Key.make[Frame](key))
    preparePartialFrame(fr, columns)
    fr.update()
  }

  override def createChunk(keyName: String, numRows: Int, expectedTypes: Array[Byte], chunkId: Int,
                           maxVecSizes: Array[Int], sparse: Array[Boolean], vecStartSize: Map[Int, Int]): Unit = {
    chunks = FrameUtils.createNewChunks(keyName, expectedTypes, chunkId, sparse)
    sparseVectorPts = collection.mutable.Map(vecStartSize.mapValues(size => new Array[Int](size)).toSeq: _*)
    sparseVectorInUse = collection.mutable.Map(vecStartSize.mapValues(_ => false).toSeq: _*)
  }

  override def finalizeFrame(key: String, rowsPerChunk: Array[Long], colTypes: Array[Byte]): Unit = {
    val fr = DKV.getGet[Frame](key)
    water.fvec.FrameUtils.finalizePartialFrame(fr, rowsPerChunk, null, colTypes)
    InternalWriteConverterCtx.logChunkLocations(fr)
    InternalWriteConverterCtx.validateFrame(fr)
  }

  override def closeChunks(numRows: Int): Unit = {
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

  override def numOfRows(): Int = chunks(0).len()

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

  override def finishRow(): Unit = {}

  override def putDenseVector(startIdx: Int, vector: DenseVector, maxVecSize: Int): Unit = {
    (0 until vector.size).foreach { idx => put(startIdx + idx, vector(idx)) }

    (vector.size until maxVecSize).foreach(idx => put(startIdx + idx, 0.0))
  }
}

object InternalWriteConverterCtx {
  private def logChunkLocations(fr: Frame): Unit = {
    if (Log.isLoggingFor("DEBUG")) {
      if (!fr.vecs().isEmpty) {
        Log.debug("Number of chunks on frame: " + fr.anyVec.nChunks)
        val nodes = mutable.Map.empty[H2ONode, Int]
        (0 until fr.anyVec().nChunks()).foreach { i =>
          val home = fr.anyVec().chunkKey(i).home_node()
          if (!nodes.contains(home)) {
            nodes += (home -> 0)
          }
          nodes(home) = nodes(home) + 1
        }
        Log.debug("Frame distributed on nodes:")
        nodes.foreach {
          case (node, n) =>
            Log.debug(node + ": " + n + " chunks.")
        }
      }
    }
  }

  private def validateFrame(fr: Frame): Unit = {
    checkNumberOfChunksInEachVector(fr)
    checkESPCIsSameInEachVector(fr)
    checkNumberOfEntriesInEachChunk(fr)
  }

  private def checkNumberOfEntriesInEachChunk(fr: Frame): Unit = {
    if (!fr.vecs().isEmpty) {
      new MRTask() {
        override def map(cs: Array[Chunk]): Unit = {
          val values = cs.map(_.len())
          if (values.length > 1) {
            val firstLen = values.head
            values.tail.zipWithIndex.foreach { case (len, idx) =>
              if (firstLen != len) {
                throw new IllegalArgumentException(s"Chunks have different sizes in different vectors: $firstLen in vector 1 and" +
                  s"$len in vector $idx")
              }
            }
          }
        }
      }.doAll(fr)
    }
  }

  private def checkESPCIsSameInEachVector(fr: Frame): Unit = {
    if (!fr.vecs().isEmpty) {
      val layouts = fr.vecs().map(_.espc())
      val first = layouts.head
      layouts.tail.foreach { espc =>
        if (first != espc) {
          throw new IllegalArgumentException(
            s"""
               | Invalid shape of H2O Frame:
               |
               | ${layouts.zipWithIndex.map { case (arr, idx) => s"Vec $idx has layout: ${arr.mkString(", ")}\n" }}
               |
        }
        """.stripMargin)
        }
      }
    }
  }

  private def checkNumberOfChunksInEachVector(fr: Frame): Unit = {
    if (!fr.vecs().isEmpty) {
      val first = fr.vecs.head
      fr.vecs.tail.foreach { vec =>
        if (vec.nChunks() != first.nChunks()) {
          throw new IllegalArgumentException(
            s"Vectors have different number of chunks: ${fr.vecs().map(_.nChunks()).mkString(", ")}")
        }
      }
    }
  }
}
