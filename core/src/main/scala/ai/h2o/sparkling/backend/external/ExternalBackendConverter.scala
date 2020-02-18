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

import ai.h2o.sparkling.backend.shared.Converter
import ai.h2o.sparkling.backend.shared.Converter.{ConversionFunction, SparkJob, UploadPlan}
import ai.h2o.sparkling.frame.H2OFrame
import org.apache.spark.ExposeUtils
import org.apache.spark.h2o.utils.ReflectionUtils
import org.apache.spark.h2o.utils.SupportedTypes.{Double, Long}
import org.apache.spark.h2o.{H2OContext, _}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.sql.types.{DataType, DateType, DecimalType}
import water._

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


object ExternalBackendConverter extends Converter {

  /**
   * Converts the RDD to H2O Frame using specified conversion function
   *
   * @param hc            H2O context
   * @param rdd           rdd to convert
   * @param keyName       key of the resulting frame
   * @param colNames      names of the columns in the H2O Frame
   * @param expectedTypes expected types of the vectors in the H2O Frame
   * @param maxVecSizes   maximal sizes of each element in the RDD
   * @param sparse        if at least one column in the dataset is sparse
   * @param func          conversion function - the function takes parameters needed extra by specific transformations
   *                      and returns function which does the general transformation
   * @tparam T type of RDD to convert
   * @return H2OFrame Key
   */
  def convert[T: ClassTag : TypeTag](hc: H2OContext, rdd: RDD[T], keyName: String, colNames: Array[String], expectedTypes: Array[Byte],
                                     maxVecSizes: Array[Int], sparse: Array[Boolean], func: ConversionFunction[T]): String = {
    val conf = hc.getConf
    H2OFrame.initializeFrame(conf, keyName, colNames)

    val partitionSizes = getNonEmptyPartitionSizes(rdd)
    val nonEmptyPartitions = getNonEmptyPartitions(partitionSizes)
    // prepare required metadata
    val uploadPlan = Some(scheduleUpload(nonEmptyPartitions.size))
    val operation: SparkJob[T] = func(keyName, expectedTypes, uploadPlan, sparse, nonEmptyPartitions, partitionSizes)
    val rows = hc.sparkContext.runJob(rdd, operation, nonEmptyPartitions) // eager, not lazy, evaluation
    val res = new Array[Long](nonEmptyPartitions.size)
    rows.foreach { case (cidx, nrows) => res(cidx) = nrows }
    // Add Vec headers per-Chunk, and finalize the H2O Frame

    // get the vector types from expected types
    val types = ExternalFrameUtils.vecTypesFromExpectedTypes(expectedTypes, maxVecSizes)
    H2OFrame.finalizeFrame(conf, keyName, res, types)
    keyName
  }

  private def scheduleUpload(numPartitions: Int): UploadPlan = {
    val nodes = H2OContext.ensure("H2OContext needs to be running").getH2ONodes()
    val uploadPlan = (0 until numPartitions).zip(Stream.continually(nodes).flatten).toMap
    uploadPlan
  }

  def internalJavaClassOf(dt: DataType): Class[_] = {
    dt match {
      case n if n.isInstanceOf[DecimalType] & n.getClass.getSuperclass != classOf[DecimalType] => Double.javaClass
      case _: DateType => Long.javaClass
      case v if ExposeUtils.isAnyVectorUDT(v) => classOf[Vector]
      case _: DataType => ReflectionUtils.supportedTypeOf(dt).javaClass
    }
  }
}
