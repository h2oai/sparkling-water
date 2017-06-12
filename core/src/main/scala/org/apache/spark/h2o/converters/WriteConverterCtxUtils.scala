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
import org.apache.spark.h2o.backends.external.ExternalWriteConverterCtx
import org.apache.spark.h2o.backends.internal.InternalWriteConverterCtx
import org.apache.spark.h2o.utils.NodeDesc
import org.apache.spark.h2o.{H2OContext, _}
import org.apache.spark.rdd.h2o.H2OAwareRDD
import water.fvec.H2OFrame
import water.{DKV, ExternalFrameUtils, Key}

import scala.collection.immutable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


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
    * @param hc       H2O context
    * @param rddInput rdd to convert
    * @param keyName  key of the resulting frame
    * @param colNames names of the columns in the H2O Frame
    * @param vecTypes types of the vectors in the H2O Frame
    * @param func     conversion function - the function takes parameters needed extra by specific transformations
    *                 and returns function which does the general transformation
    * @tparam T type of RDD to convert
    * @return H2O Frame
    */

  def convert[T: ClassTag: TypeTag](hc: H2OContext, rddInput: RDD[T], keyName: String, colNames: Array[String], vecTypes: Array[Byte],
                                    maxVecSizes: Array[Int], func: ConversionFunction[T]) = {
    // create new RDD which is located only on the nodes wit h2o
    // Make an H2O data Frame - but with no backing data (yet)
    initFrame(keyName, colNames)

    // prepare required metadata based on the used backend
    val uploadPlan = if (hc.getConf.runsInExternalClusterMode) {
      Some(ExternalWriteConverterCtx.scheduleUpload(rddInput.getNumPartitions))
    } else {
      None
    }
    val rdd = if (hc.getConf.runsInInternalClusterMode) {
      // this is only required in internal cluster mode
      val prefs = hc.h2oNodes.map{ nodeDesc =>
        s"executor_${nodeDesc.hostname}_${nodeDesc.nodeId}"
      }
      new H2OAwareRDD(prefs, rddInput)
    } else {
      rddInput
    }

    val operation: SparkJob[T] = func(keyName, vecTypes, uploadPlan, hc.getConf.externalWriteConfirmationTimeout)

    val rows = hc.sparkContext.runJob(rdd, operation) // eager, not lazy, evaluation
    val res = new Array[Long](rdd.partitions.length)
    rows.foreach { case (cidx, nrows) => res(cidx) = nrows }
    // Add Vec headers per-Chunk, and finalize the H2O Frame

    // get the vector types from expected types in case of external h2o cluster
    val types = if (hc.getConf.runsInExternalClusterMode) {
      ExternalFrameUtils.vecTypesFromExpectedTypes(vecTypes, maxVecSizes)
    } else {
      vecTypes
    }
    new H2OFrame(finalizeFrame(keyName, res, types))
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
