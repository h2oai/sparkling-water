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

import ai.h2o.sparkling.backend.shared.Converter
import ai.h2o.sparkling.backend.shared.Converter.{ConversionFunction, SparkJob}
import org.apache.spark.h2o.{H2OContext, _}
import water._
import water.fvec.Chunk
import water.fvec.FrameUtils.preparePartialFrame
import water.util.Log

import scala.collection.mutable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


object InternalBackendConverter extends Converter {

  /**
   * Converts the RDD to H2O Frame using specified conversion function
   *
   * @param hc            H2O context
   * @param rddInput      rdd to convert
   * @param keyName       key of the resulting frame
   * @param colNames      names of the columns in the H2O Frame
   * @param expectedTypes expected types of the vectors in the H2O Frame
   * @param maxVecSizes   maximal sizes of each element in the RDD
   * @param sparse        if at least one column in the dataset is sparse
   * @param func          conversion function - the function takes parameters needed extra by specific transformations
   *                      and returns function which does the general transformation
   * @tparam T type of RDD to convert
   * @return H2O Frame
   */
  def convert[T: ClassTag : TypeTag](hc: H2OContext, rddInput: RDD[T], keyName: String, colNames: Array[String], expectedTypes: Array[Byte],
                                     maxVecSizes: Array[Int], sparse: Array[Boolean], func: ConversionFunction[T]): String = {
    initFrame(keyName, colNames)
    val rdd = new H2OAwareRDD(hc.getH2ONodes(), rddInput)
    val partitionSizes = getNonEmptyPartitionSizes(rdd)
    val nonEmptyPartitions = getNonEmptyPartitions(partitionSizes)
    val operation: SparkJob[T] = func(keyName, expectedTypes, None, sparse, nonEmptyPartitions, partitionSizes)
    val rows = hc.sparkContext.runJob(rdd, operation, nonEmptyPartitions) // eager, not lazy, evaluation
    val res = new Array[Long](nonEmptyPartitions.size)
    rows.foreach { case (cidx, nrows) => res(cidx) = nrows }

    finalizeFrame(keyName, res, expectedTypes)
    keyName
  }

  private def finalizeFrame(key: String, rowsPerChunk: Array[Long], colTypes: Array[Byte]): Unit = {
    val fr = DKV.getGet[Frame](key)
    water.fvec.FrameUtils.finalizePartialFrame(fr, rowsPerChunk, null, colTypes)
    logChunkLocations(fr)
    validateFrame(fr)
  }

  private def initFrame(key: String, columns: Array[String]): Unit = {
    val fr = new water.fvec.Frame(Key.make[Frame](key))
    preparePartialFrame(fr, columns)
    fr.update()
  }

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
