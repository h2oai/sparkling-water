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

import ai.h2o.sparkling.backend.external.{ExternalBackendConverter, ExternalBackendWriter}
import ai.h2o.sparkling.backend.internal.{InternalBackendConverter, InternalBackendWriter}
import ai.h2o.sparkling.backend.shared.Converter.ConversionFunction
import org.apache.spark.TaskContext
import org.apache.spark.h2o.utils.NodeDesc
import org.apache.spark.h2o.{H2OContext, _}

import scala.collection.immutable
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

trait Converter {
  protected def getNonEmptyPartitionSizes[T](rdd: RDD[T]): Map[Int, Int] = {
    rdd.mapPartitionsWithIndex {
      case (idx, it) => if (it.nonEmpty) {
        Iterator.single((idx, it.size))
      } else {
        Iterator.empty
      }
    }.collect().toMap
  }

  protected def getNonEmptyPartitions(partitionSizes: Map[Int, Int]): Seq[Int] = {
    partitionSizes.keys.toSeq.sorted
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
  def convert[T: ClassTag : TypeTag](hc: H2OContext, rddInput: RDD[T], keyName: String, colNames: Array[String], expectedTypes: Array[Byte],
                                     maxVecSizes: Array[Int], sparse: Array[Boolean], func: ConversionFunction[T]): String

}

object Converter {
  type SparkJob[T] = (TaskContext, Iterator[T]) => (Int, Long)
  type ConversionFunction[T] = (String, Array[Byte], Option[UploadPlan], Array[Boolean], Seq[Int], Map[Int, Int]) => SparkJob[T]
  type UploadPlan = immutable.Map[Int, NodeDesc]

  def createWriter(conf: H2OConf,
                   uploadPlan: Option[UploadPlan],
                   chunkIdx: Int,
                   frameName: String,
                   numRows: Int,
                   expectedTypes: Array[Byte],
                   maxVecSizes: Array[Int],
                   sparse: Array[Boolean],
                   vecStartSize: Map[Int, Int]): Writer = {
    uploadPlan
      .map { _ => new ExternalBackendWriter(conf, uploadPlan.get(chunkIdx), frameName, numRows, expectedTypes, chunkIdx, maxVecSizes, sparse) }
      .getOrElse(new InternalBackendWriter(frameName, numRows, expectedTypes, chunkIdx, sparse, vecStartSize))
  }

  def getConverter(conf: H2OConf): Converter = {
    if (conf.runsInExternalClusterMode) {
      ExternalBackendConverter
    } else {
      InternalBackendConverter
    }
  }
}
