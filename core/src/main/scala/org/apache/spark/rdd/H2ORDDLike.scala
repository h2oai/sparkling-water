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

package org.apache.spark.rdd

import org.apache.spark.Partition
import water.fvec.{Chunk, Frame}
import water.{DKV, Key}

/**
 * Contains functions that are shared between all H2ORDD types (i.e., Scala, Java)
 */
private[rdd] trait H2ORDDLike[T <: Frame] {
  /** Underlying DataFrame */
  @transient val frame: T

  /** Cache frame key to get H2OFrame from the K/V store */
  val frameKeyName: String = frame._key.toString

  /** Number of chunks per a vector */
  val numChunks: Int = frame.anyVec().nChunks()

  protected def getPartitions: Array[Partition] = {
    val res = new Array[Partition](numChunks)
    for(i <- 0 until numChunks) res(i) = new Partition { val index = i }
    res
  }

  /** Base implementation for iterator over rows stored in chunks for given partition. */
  trait H2OChunkIterator[+A] extends Iterator[A] {
    /* Key of pointing to underlying dataframe */
    val keyName: String
    /* Partition index */
    val partIndex: Int
    /* Lazily fetched dataframe from K/V store */
    lazy val fr: Frame = getFrame()
    /* Chunks for this partition */
    lazy val chks: Array[Chunk] = water.fvec.FrameUtils.getChunks(fr, partIndex)
    /* Number of rows in this partition */
    lazy val nrows = chks(0)._len
    /* Number of columns in the dataset */
    lazy val ncols = fr.numCols()

    /* Iterator state: Actual row */
    var row: Int = 0

    def hasNext: Boolean = row < nrows

    private def getFrame() = DKV.get(Key.make(keyName)).get.asInstanceOf[Frame]
  }

}
