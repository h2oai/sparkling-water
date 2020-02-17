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

import java.io.Closeable

import org.apache.spark._

/**
 * Methods which each Writer has to implement.
 *
 * Writer is used to transfer data into specific H2O backend
 */
trait Writer extends Closeable {

  def startRow(rowIdx: Int)

  def put(colIdx: Int, data: Boolean)

  def put(colIdx: Int, data: Byte)

  def put(colIdx: Int, data: Char)

  def put(colIdx: Int, data: Short)

  def put(colIdx: Int, data: Int)

  def put(colIdx: Int, data: Long)

  def put(colIdx: Int, data: Float)

  def put(colIdx: Int, data: Double)

  def put(colIdx: Int, data: java.sql.Timestamp)

  def put(colIdx: Int, data: java.sql.Date)

  def put(colIdx: Int, data: String)

  def putNA(colIdx: Int, sparkIdx: Int)

  def putSparseVector(startIdx: Int, vector: ml.linalg.SparseVector, maxVecSize: Int)

  def putDenseVector(startIdx: Int, vector: ml.linalg.DenseVector, maxVecSize: Int)

  def putVector(startIdx: Int, vec: mllib.linalg.Vector, maxVecSize: Int): Unit = putVector(startIdx, vec.asML, maxVecSize)

  def putVector(startIdx: Int, vec: ml.linalg.Vector, maxVecSize: Int): Unit = {
    vec match {
      case sparseVector: ml.linalg.SparseVector =>
        putSparseVector(startIdx, sparseVector, maxVecSize)
      case denseVector: ml.linalg.DenseVector =>
        putDenseVector(startIdx, denseVector, maxVecSize)
    }
  }
}
