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
import org.apache.spark.h2o.backends.external.{ExternalBackendUtils, ExternalWriteConverterCtx}
import org.apache.spark.h2o.converters.WriteConverterCtxUtils.UploadPlan
import org.apache.spark.h2o.utils.ReflectionUtils
import org.apache.spark.internal.Logging
import water.Key
import water.fvec.H2OFrame

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

private[converters] object PrimitiveRDDConverter extends Logging {

  def toH2OFrame[T: ClassTag : TypeTag](hc: H2OContext, rdd: RDD[T], frameKeyName: Option[String]): H2OFrame = {
    import ReflectionUtils._

    val keyName = frameKeyName.getOrElse("frame_rdd_" + rdd.id + Key.rand())

    val fnames = Array[String]("values")

    // in case of internal backend, store regular vector types
    // otherwise for external backend store expected types
    val expectedTypes = if (hc.getConf.runsInInternalClusterMode) {
      Array[Byte](vecTypeOf[T])
    } else {
      val clazz = ExternalWriteConverterCtx.internalJavaClassOf[T]
      ExternalBackendUtils.prepareExpectedTypes(Array[Class[_]](clazz))
    }

    WriteConverterCtxUtils.convert[T](hc, rdd, keyName, fnames, expectedTypes, Array.empty[Int], perPrimitiveRDDPartition())
  }


  /**
    *
    * @param keyName    key of the frame
    * @param vecTypes   h2o vec types
    * @param uploadPlan if external backend is used, then it is a plan which assigns each partition h2o
    *                   node where the data from that partition will be uploaded, otherwise is Node
    * @param context    spark task context
    * @param it         iterator over data in the partition
    * @tparam T type of data inside the RDD
    * @return pair (partition ID, number of rows in this partition)
    */
  private[this]
  def perPrimitiveRDDPartition[T]() // extra arguments for this transformation
                                 (keyName: String, vecTypes: Array[Byte], uploadPlan: Option[UploadPlan], writeTimeout: Int) // general arguments
                                 (context: TaskContext, it: Iterator[T]): (Int, Long) = { // arguments and return types needed for spark's runJob input

    val (iterator, dataSize) = WriteConverterCtxUtils.bufferedIteratorWithSize(uploadPlan, it)
    val con = WriteConverterCtxUtils.create(uploadPlan, context.partitionId(), dataSize, writeTimeout)

    con.createChunks(keyName, vecTypes, context.partitionId(), Array.empty[Int])
    iterator.foreach {
      con.putAnySupportedType(0, _)
    }
    //Compress & write data in partitions to H2O Chunks
    con.closeChunks()
    // Return Partition number and number of rows in this partition
    (context.partitionId, con.numOfRows())
  }

}
