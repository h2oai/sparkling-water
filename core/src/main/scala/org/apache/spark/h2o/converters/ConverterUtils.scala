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

import org.apache.spark.TaskContext
import org.apache.spark.h2o._
import org.apache.spark.h2o.backends.internal.{InternalReadConverterContext, InternalWriteConverterContext}
import org.apache.spark.h2o.utils.NodeDesc
import water.{DKV, Key}

import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.reflect.runtime.universe._


//private[converters] - will uncomment when H2OContext moves into converters
trait ConverterUtils {


  def initFrame[T](keyName: String, names: Array[String]):Unit = {
    val fr = new water.fvec.Frame(Key.make(keyName))
    water.fvec.FrameUtils.preparePartialFrame(fr, names)
    // Save it directly to DKV
    fr.update()
  }


  def finalizeFrame[T](keyName: String,
                       res: Array[Long],
                       colTypes: Array[Byte],
                       colDomains: Array[Array[String]] = null):Frame = {
    val fr:Frame = DKV.get(keyName).get.asInstanceOf[Frame]
    water.fvec.FrameUtils.finalizePartialFrame(fr, res, colDomains, colTypes)
    fr
  }

  /**
    * Gets frame for specified key or none if that frame does not exist
 *
    * @param keyName key of the requested frame
    * @return option containing frame or none
    */
  def getFrameOrNone(keyName: String): Option[H2OFrame] = {
    // Fetch cached frame from DKV
    val frameVal = DKV.get(keyName)

    // TODO(vlad): get rid of casting, take care of failures
    Option(frameVal) map (v => new H2OFrame(v.get.asInstanceOf[Frame]))
  }

  import ConverterUtils._

  /**
    * Converts the RDD to H2O Frame using specified conversion function
    *
    * @param hc H2O context
    * @param rdd rdd to convert
    * @param keyName key of the resulting frame
    * @param colNames names of the columns in the H2O Frame
    * @param vecTypes types of the vectors in the H2O Frame
    * @param func conversion function - the function takes parameters needed extra by specific transformations
    *             and returns function which does the general transformation
    * @tparam T type of RDD to convert
    * @return H2O Frame
    */
  def convert[T](hc: H2OContext, rdd : RDD[T], keyName: String, colNames: Array[String], vecTypes: Array[Byte],
                 func: ConversionFunction[T]) = {
    // Make an H2O data Frame - but with no backing data (yet)
    initFrame(keyName, colNames)

    // prepare rdd and required metadata based on the used backend
    val (preparedRDD, uploadPlan) = (rdd, None)

    val operation: SparkJob[T] = func(keyName, vecTypes, uploadPlan)

    val rows = hc.sparkContext.runJob(preparedRDD, operation) // eager, not lazy, evaluation
    val res = new Array[Long](preparedRDD.partitions.length)
    rows.foreach { case (cidx,  nrows) => res(cidx) = nrows }
    // Add Vec headers per-Chunk, and finalize the H2O Frame
    new H2OFrame(finalizeFrame(keyName, res, vecTypes))
  }
}

object ConverterUtils {

  // TODO(vlad): clean this up
  type SparkJob[T] = (TaskContext, Iterator[T]) => (Int, Long)

  type ConversionFunction[T] = (String, Array[Byte], Option[immutable.Map[Int, NodeDesc]]) => SparkJob[T]


  def getWriteConverterContext(uploadPlan: Option[immutable.Map[Int, NodeDesc]],
                               partitionId: Int): WriteConverterContext = {
    val converterContext = new InternalWriteConverterContext()
    converterContext
  }

  def getReadConverterContext(isExternalBackend: Boolean,
                              keyName: String,
                              chksLocation: Option[Array[NodeDesc]],
                              types: Option[Array[Byte]],
                              chunkIdx: Int): ReadConverterContext = {
    val converterContext = new InternalReadConverterContext(keyName, chunkIdx)
    converterContext
  }

  def getReadConverterContext(keyName: String,
                              chunkIdx: Int): ReadConverterContext = {
    val converterContext = new InternalReadConverterContext(keyName, chunkIdx)
    converterContext
  }

  // TODO(vlad): get rid of boolean; rename
  def getIterator[T](isExternalBackend: Boolean,
                     iterator: Iterator[T]): Iterator[T] = {
    if (isExternalBackend) {
      val rows = new ListBuffer[T]()
      while (iterator.hasNext) {
        rows += iterator.next()
      }
      rows.iterator
    } else {
      iterator
    }
  }

  def prepareExpectedTypes[T: TypeTag](isExternalBackend: Boolean,
                                       types: Array[T]): Option[Array[Byte]] = {
    // For now return None because internal backend is used at all cases and we don't need any additional info at this time.
    None
  }

}

