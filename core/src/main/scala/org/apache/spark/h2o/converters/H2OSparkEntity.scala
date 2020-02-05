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

import ai.h2o.sparkling.frame.H2OFrame
import org.apache.spark.Partition
import org.apache.spark.h2o.H2OConf
import org.apache.spark.h2o.utils.NodeDesc
import water.fvec.{Frame, FrameUtils}

import scala.annotation.meta.{field, getter}

/**
  * Contains functions that are shared between all H2O DataFrames and RDDs.
  */
private[converters] trait H2OSparkEntity {

  /** Is the external backend in use */
  val isExternalBackend: Boolean

  /** Timestamp of the H2O Driver node */
  val driverTimeStamp: Short

  /** Cache frame key to get H2OFrame from the K/V store */
  def frameKeyName: String

  /** Number of chunks per a vector */
  def numChunks: Int

  /** Chunk locations helps us to determine the node which really has the data we needs. */
  def chksLocation : Option[Array[NodeDesc]]

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

    val conf: H2OConf
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
        chksLocation, expectedTypes, selectedColumnIndices, conf
      )

    override def hasNext: Boolean = converterCtx.hasNext
  }
}

/**
  * Contains functions that are shared between all client-based H2O DataFrames and RDDs.
  */
private[converters] trait H2OClientBasedSparkEntity[T <: Frame] extends H2OSparkEntity {
  /** Underlying DataFrame */
  @(transient @field @getter) val frame: T

  /** Cache frame key to get H2OFrame from the K/V store */
  override val frameKeyName: String = frame._key.toString

  /** Number of chunks per a vector */
  override val numChunks: Int = frame.anyVec().nChunks()

  /** Chunk locations helps us to determine the node which really has the data we needs. */
  override val chksLocation = if (isExternalBackend) Some(FrameUtils.getChunksLocations(frame)) else None
}

/**
  * Contains functions that are shared between all REST-based H2ORDD types (i.e., Scala, Java)
  */
private[converters] trait H2ORESTBasedSparkEntity extends H2OSparkEntity {
  /** Underlying H2O Frame */
  val frame: H2OFrame

  /** Timestamp of the H2O Driver node */
  override val driverTimeStamp: Short = -1 // Setting timestamp to -1 since there is no H2O client running

  /** Cache frame key to get H2OFrame from the H2O backend */
  override val frameKeyName: String = frame.frameId

  /** Number of chunks per a vector */
  override val numChunks: Int = frame.chunks.length

  /** Chunk locations helps us to determine the node which really has the data we needs. */
  override val chksLocation: Option[Array[NodeDesc]] = Some(frame.chunks.map(_.location))
}


