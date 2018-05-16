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

import org.apache.spark.h2o.backends.external.ExternalWriteConverterCtx
import org.apache.spark.h2o.backends.internal.InternalWriteConverterCtx
import org.apache.spark.h2o.utils.NodeDesc
import org.apache.spark.h2o.{H2OContext, _}
import org.apache.spark.TaskContext
import org.apache.spark.h2o.utils.LogUtil.setLogLevel
import water.fvec.{Chunk, H2OFrame}
import water._
import water.util.Log

import scala.collection.{immutable, mutable}


object WriteConverterCtxUtils {

  type SparkJob[T] = (TaskContext, Iterator[T]) => (Int, Long)
  type ConversionFunction[T] = (String, Array[Byte], Option[UploadPlan], Int) => SparkJob[T]
  type UploadPlan = immutable.Map[Int, NodeDesc]

  def create(uploadPlan: Option[UploadPlan],
             partitionId: Int, totalNumOfRows: Option[Int], writeTimeout: Int): WriteConverterCtx = {
    uploadPlan
      .map { _ => new ExternalWriteConverterCtx(uploadPlan.get(partitionId), totalNumOfRows.get, writeTimeout) }
      .getOrElse(new InternalWriteConverterCtx())
  }

  /**
    * This method is used for writing data from spark partitions to h2o chunks.
    *
    * In case of internal backend it returns the original iterator and empty length because we do not need it
    * In case of external backend it returns new iterator with the same data and the length of the data
    */
  def bufferedIteratorWithSize[T](uploadPlan: Option[UploadPlan], original: Iterator[T]): (Iterator[T], Option[Int]) = {
    uploadPlan.map { _ =>
      val buffered = original.toList
      (buffered.iterator, Some(buffered.size))
    }.getOrElse(original, None)
  }

  /**
    * Converts the RDD to H2O Frame using specified conversion function
    *
    * @param hc            H2O context
    * @param rdd           rdd to convert
    * @param keyName       key of the resulting frame
    * @param colNames      names of the columns in the H2O Frame
    * @param expectedTypes expected types of the vectors in the H2O Frame
    * @param func          conversion function - the function takes parameters needed extra by specific transformations
    *                      and returns function which does the general transformation
    * @tparam T type of RDD to convert
    * @return H2O Frame
    */

  def convert[T](hc: H2OContext, rdd: RDD[T], keyName: String, colNames: Array[String], expectedTypes: Array[Byte],
                 maxVecSizes: Array[Int], func: ConversionFunction[T]) = {
    // Make an H2O data Frame - but with no backing data (yet)
    initFrame(keyName, colNames)

    // prepare required metadata based on the used backend
    val uploadPlan = if (hc.getConf.runsInExternalClusterMode) {
      Some(ExternalWriteConverterCtx.scheduleUpload(rdd.getNumPartitions))
    } else {
      None
    }

    val operation: SparkJob[T] = func(keyName, expectedTypes, uploadPlan, hc.getConf.externalWriteConfirmationTimeout)

    val rows = hc.sparkContext.runJob(rdd, operation) // eager, not lazy, evaluation
    val res = new Array[Long](rdd.partitions.length)
    rows.foreach { case (cidx, nrows) => res(cidx) = nrows }
    // Add Vec headers per-Chunk, and finalize the H2O Frame

    // get the vector types from expected types in case of external h2o cluster
    val types = if (hc.getConf.runsInExternalClusterMode) {
      ExternalFrameUtils.vecTypesFromExpectedTypes(expectedTypes, maxVecSizes)
    } else {
      expectedTypes
    }
    val fr = new H2OFrame(finalizeFrame(keyName, res, types))

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

    // Validate num of chunks in each vector
    if (!fr.vecs().isEmpty) {
      val first = fr.vecs.head
      fr.vecs.tail.foreach { vec =>
        if (vec.nChunks() != first.nChunks()) {
          throw new IllegalArgumentException(
            s"""
               | Vectors have different number of chunks: ${fr.vecs().map(_.nChunks()).mkString(", ")}
        """.stripMargin)
        }
      }
    }

    // Validate that espc is the same in each vector
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

    // Check number of entries in each chunk
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

    fr
  }

  private def initFrame(keyName: String, names: Array[String]): Unit = {
    val fr = new water.fvec.Frame(Key.make[Frame](keyName))
    water.fvec.FrameUtils.preparePartialFrame(fr, names)
    // Save it directly to DKV
    fr.update()
  }

  private def finalizeFrame(keyName: String,
                            res: Array[Long],
                            colTypes: Array[Byte],
                            colDomains: Array[Array[String]] = null): Frame = {

    val fr = DKV.getGet[Frame](keyName)
    water.fvec.FrameUtils.finalizePartialFrame(fr, res, colDomains, colTypes)
    fr
  }

}
