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

import ai.h2o.sparkling.utils.RestApiUtils
import org.apache.spark.TaskContext
import org.apache.spark.h2o.backends.external.ExternalWriteConverterCtx
import org.apache.spark.h2o.backends.internal.InternalWriteConverterCtx
import org.apache.spark.h2o.utils.NodeDesc
import org.apache.spark.h2o.{H2OContext, _}
import org.apache.spark.rdd.h2o.H2OAwareRDD
import water.{ExternalFrameUtils, _}

import scala.collection.immutable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


object WriteConverterCtxUtils {

  type SparkJob[T] = (TaskContext, Iterator[T]) => (Int, Long)
  type ConversionFunction[T] = (String, Array[Byte], Option[UploadPlan], Array[Boolean], Seq[Int]) => SparkJob[T]
  type UploadPlan = immutable.Map[Int, NodeDesc]

  def create(conf: H2OConf, uploadPlan: Option[UploadPlan], partitionId: Int): WriteConverterCtx = {
    uploadPlan
      .map { _ => new ExternalWriteConverterCtx(conf, uploadPlan.get(partitionId)) }
      .getOrElse(new InternalWriteConverterCtx())
  }

  /**
    * This method is used for writing data from spark partitions to h2o chunks.
    *
    * In case of internal backend it returns the original iterator and empty length because we do not need it
    * In case of external backend it returns new iterator with the same data and the length of the data
    */
  def bufferedIteratorWithSize[T](uploadPlan: Option[UploadPlan], original: Iterator[T]): (Iterator[T], Int) = {
    uploadPlan.map { _ =>
      val buffered = original.toList
      (buffered.iterator, buffered.size)
    }.getOrElse(original, -1)
  }

  trait Converter {
    protected def getNonEmptyPartitions[T](rdd: RDD[T]): Seq[Int] = {
      rdd.mapPartitionsWithIndex {
        case (idx, it) => if (it.nonEmpty) Iterator.single(idx) else Iterator.empty
      }.collect().toSeq.sorted
    }

    /**
      * Converts the RDD to H2O Frame using specified conversion function
      *
      * @param hc            H2O context
      * @param rddInput      rdd to convert
      * @param keyName       key of the resulting frame
      * @param colNames      names of the columns in the H2O Frame
      * @param expectedTypes expected types of the vectors in the H2O Frame
      * @param sparse        if at least one column in the dataset is sparse
      * @param func          conversion function - the function takes parameters needed extra by specific transformations
      *                      and returns function which does the general transformation
      * @tparam T type of RDD to convert
      * @return H2O Frame
      */
    def convert[T: ClassTag: TypeTag](hc: H2OContext, rddInput: RDD[T], keyName: String, colNames: Array[String], expectedTypes: Array[Byte],
                                      maxVecSizes: Array[Int], sparse: Array[Boolean], func: ConversionFunction[T]): String
  }

  def getConverter(conf: H2OConf): Converter = {
    if (conf.runsInExternalClusterMode) {
      WriteConverterCtxUtils.ExternalBackendConverter
    } else {
      WriteConverterCtxUtils.InternalBackendConverter
    }
  }

  object InternalBackendConverter extends Converter {

    /**
      * Converts the RDD to H2O Frame using specified conversion function
      *
      * @param hc            H2O context
      * @param rddInput      rdd to convert
      * @param keyName       key of the resulting frame
      * @param colNames      names of the columns in the H2O Frame
      * @param expectedTypes expected types of the vectors in the H2O Frame
      * @param sparse        if at least one column in the dataset is sparse
      * @param func          conversion function - the function takes parameters needed extra by specific transformations
      *                      and returns function which does the general transformation
      * @tparam T type of RDD to convert
      * @return H2O Frame
      */
    def convert[T: ClassTag : TypeTag](hc: H2OContext, rddInput: RDD[T], keyName: String, colNames: Array[String], expectedTypes: Array[Byte],
                                       maxVecSizes: Array[Int], sparse: Array[Boolean], func: ConversionFunction[T]): String = {
      val writerClient = new InternalWriteConverterCtx()
      writerClient.initFrame(keyName, colNames)

      val prefs = hc.getH2ONodes().map(nodeDesc => s"executor_${nodeDesc.hostname}_${nodeDesc.nodeId}")
      val rdd = new H2OAwareRDD(prefs, rddInput)
      val nonEmptyPartitions = getNonEmptyPartitions(rdd)
      val operation: SparkJob[T] = func(keyName, expectedTypes, None, sparse, nonEmptyPartitions)
      val rows = hc.sparkContext.runJob(rdd, operation, nonEmptyPartitions) // eager, not lazy, evaluation
      val res = new Array[Long](nonEmptyPartitions.size)
      rows.foreach { case (cidx, nrows) => res(cidx) = nrows }

      writerClient.finalizeFrame(keyName, res, expectedTypes)
      keyName
    }

  }

  object ExternalBackendConverter extends Converter {

    /**
      * Converts the RDD to H2O Frame using specified conversion function
      *
      * @param hc            H2O context
      * @param rdd           rdd to convert
      * @param keyName       key of the resulting frame
      * @param colNames      names of the columns in the H2O Frame
      * @param expectedTypes expected types of the vectors in the H2O Frame
      * @param sparse        if at least one column in the dataset is sparse
      * @param func          conversion function - the function takes parameters needed extra by specific transformations
      *                      and returns function which does the general transformation
      * @tparam T type of RDD to convert
      * @return H2OFrame Key
      */
    def convert[T: ClassTag : TypeTag](hc: H2OContext, rdd: RDD[T], keyName: String, colNames: Array[String], expectedTypes: Array[Byte],
                                       maxVecSizes: Array[Int], sparse: Array[Boolean], func: ConversionFunction[T]): String = {
      val conf = hc.getConf
      val leaderNode = RestApiUtils.getLeaderNode(conf)
      val writerClient = new ExternalWriteConverterCtx(conf, leaderNode)

      writerClient.initFrame(keyName, colNames)

      val nonEmptyPartitions = getNonEmptyPartitions(rdd)

      // prepare required metadata
      val uploadPlan = Some(ExternalWriteConverterCtx.scheduleUpload(nonEmptyPartitions.size))

      val operation: SparkJob[T] = func(keyName, expectedTypes, uploadPlan, sparse, nonEmptyPartitions)
      val rows = hc.sparkContext.runJob(rdd, operation, nonEmptyPartitions) // eager, not lazy, evaluation
      val res = new Array[Long](nonEmptyPartitions.size)
      rows.foreach { case (cidx, nrows) => res(cidx) = nrows }
      // Add Vec headers per-Chunk, and finalize the H2O Frame

      // get the vector types from expected types
      val types = ExternalFrameUtils.vecTypesFromExpectedTypes(expectedTypes, maxVecSizes)
      writerClient.finalizeFrame(keyName, res, types)
      keyName
    }
  }
}
