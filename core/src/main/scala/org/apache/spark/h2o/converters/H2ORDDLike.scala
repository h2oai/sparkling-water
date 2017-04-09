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

package org.apache.spark.h2o.converters

import org.apache.spark.Partition
import water.fvec.{Frame, FrameUtils}

import scala.annotation.meta.{field, getter}

/**
 * Contains functions that are shared between all H2ORDD types (i.e., Scala, Java)
 */
private[converters] trait H2ORDDLike[T <: Frame] {
  /** Underlying DataFrame */
  @(transient @field @getter) val frame: T

  /** Is the external backend in use */
  val isExternalBackend: Boolean

  /** Timeout for read confirmation */
  val readTimeout: Int

  /** Cache frame key to get H2OFrame from the K/V store */
  val frameKeyName: String = frame._key.toString

  /** Number of chunks per a vector */
  val numChunks: Int = frame.anyVec().nChunks()

  /** Chunk locations helps us to determine the node which really has the data we needs. */
  val chksLocation = if (isExternalBackend) Some(FrameUtils.getChunksLocations(frame)) else None

  /** Selected column indices */
  val selectedColumnIndices: Array[Int]

  /** Create new types list which describes expected types in a way external H2O backend can use it. This list
  * contains types in a format same for H2ODataFrame and H2ORDD */
  val expectedTypes: Option[Array[Byte]]

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

    /* Converter context */
    lazy val converterCtx: ReadConverterCtx =
      ReadConverterCtxUtils.create(
        keyName,
        partIndex,
        // we need to send list of all expected types, not only the list filtered for expected columns
        // because on the h2o side we get the expected type using index from selectedColumnIndices array
        chksLocation, expectedTypes, selectedColumnIndices, readTimeout
      )

    override def hasNext: Boolean = converterCtx.hasNext
  }

}
