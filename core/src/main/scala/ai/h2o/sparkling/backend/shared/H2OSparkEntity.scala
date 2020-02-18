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

package ai.h2o.sparkling.backend.shared

import org.apache.spark.Partition

/**
 * Contains functions that are shared between all H2O DataFrames and RDDs.
 */
private[backend] trait H2OSparkEntity {
  /** Cache frame key to get H2OFrame from the K/V store */
  def frameKeyName: String

  /** Selected column indices */
  val selectedColumnIndices: Array[Int]

  /** Number of chunks per a vector */
  def numChunks: Int

  protected def getPartitions: Array[Partition] = {
    val res = new Array[Partition](numChunks)
    for (i <- 0 until numChunks) res(i) = new Partition {
      val index: Int = i
    }
    res
  }

  /** Base implementation for iterator over rows stored in chunks for given partition. */
  trait H2OChunkIterator[+A] extends Iterator[A] {

    val converterCtx: Reader

    override def hasNext: Boolean = converterCtx.hasNext
  }

}
