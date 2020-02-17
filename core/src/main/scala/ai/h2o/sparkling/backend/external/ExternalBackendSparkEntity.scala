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

package ai.h2o.sparkling.backend.external

import ai.h2o.sparkling.backend.shared.H2OSparkEntity
import ai.h2o.sparkling.frame.{H2OChunk, H2OFrame}

/**
 * Contains functions that are shared between all REST-based H2ORDD types (i.e., Scala, Java)
 */
private[external] trait ExternalBackendSparkEntity extends H2OSparkEntity {
  /** Underlying H2O Frame */
  val frame: H2OFrame

  /** Timestamp of the H2O Driver node */
  override val driverTimeStamp: Short = -1 // Setting timestamp to -1 since there is no H2O client running

  /** Cache frame key to get H2OFrame from the H2O backend */
  override val frameKeyName: String = frame.frameId

  /** Number of chunks per a vector */
  override val numChunks: Int = frame.chunks.length

  /** Chunk locations helps us to determine the node which really has the data we needs. */
  override val chksLocation: Option[Array[H2OChunk]] = Some(frame.chunks)
}
