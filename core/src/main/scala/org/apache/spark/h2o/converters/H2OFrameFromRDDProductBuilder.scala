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
import org.apache.spark.h2o.backends.external.{ExternalBackendUtils, ExternalWriteConverterCtx}
import org.apache.spark.h2o.converters.WriteConverterCtxUtils.UploadPlan
import org.apache.spark.h2o.utils.SupportedTypes.SupportedType
import org.apache.spark.h2o.{H2OContext, _}
import water.Key

import scala.reflect.runtime.universe._

case class MetaInfo(names: Array[String], types: Array[SupportedType]) {
  require(names.length > 0, "Empty meta info does not make sense")
  require(names.length == types.length, s"Different lengths: ${names.length} names, ${types.length} types")
  lazy val vecTypes: Array[Byte] = types map (_.vecType)
}

case class H2OFrameFromRDDProductBuilder(hc: H2OContext, rdd: RDD[Product], frameKeyName: Option[String]) {

  private[this] val defaultFieldNames = (i: Int) => "f" + i

  def withDefaultFieldNames() = {
    withFieldNames(defaultFieldNames)
  }

  def withFieldNames(fieldNames: Int => String): H2OFrame = {
    if (rdd.isEmpty()) {
      // transform empty Seq in order to create empty H2OFrame
      hc.asH2OFrame(hc.sparkContext.parallelize(Seq.empty[Int]), frameKeyName)
    } else {
      val meta = metaInfo(fieldNames)
      withMeta(meta)
    }
  }

  def withFields(fields: List[(String, Type)]): H2OFrame = {
    if (rdd.isEmpty()) {
      // transform empty Seq in order to create empty H2OFrame
      hc.asH2OFrame(hc.sparkContext.parallelize(Seq.empty[Int]), frameKeyName)
    } else {
      val meta = metaInfo(fields)
      withMeta(meta)
    }
  }

  private[this] def keyName(rdd: RDD[_], frameKeyName: Option[String]) = frameKeyName.getOrElse("frame_rdd_" + rdd.id + Key.rand())

  private[this] def withMeta(meta: MetaInfo): H2OFrame = {
    val kn: String = keyName(rdd, frameKeyName)

    // in case of internal backend, store regular vector types
    // otherwise for external backend store expected types
    val expectedTypes = if (hc.getConf.runsInInternalClusterMode) {
      meta.vecTypes
    } else {
      val javaClasses = meta.types.map(ExternalWriteConverterCtx.internalJavaClassOf(_))
      ExternalBackendUtils.prepareExpectedTypes(javaClasses)
    }

    WriteConverterCtxUtils.convert[Product](hc, rdd, kn, meta.names, expectedTypes, Array.empty[Int], H2OFrameFromRDDProductBuilder.perTypedDataPartition())
  }


  import org.apache.spark.h2o.utils.ReflectionUtils._

  def metaInfo(fieldNames: Int => String): MetaInfo = {
    val first = rdd.first()
    val fnames: Array[String] = (0 until first.productArity map fieldNames).toArray[String]

    MetaInfo(fnames, memberTypes(first))
  }

  def metaInfo(tuples: List[(String, Type)]): MetaInfo = {
    val names = tuples map (_._1) toArray
    val vecTypes = tuples map (nt => supportedTypeFor(nt._2)) toArray

    MetaInfo(names, vecTypes)
  }

}

object H2OFrameFromRDDProductBuilder{

  /**
    *
    * @param keyName key of the frame
    * @param vecTypes h2o vec types
    * @param uploadPlan plan which assigns each partition h2o node where the data from that partition will be uploaded
    * @param context spark task context
    * @param it iterator over data in the partition
    * @tparam T type of data inside the RDD
    * @return pair (partition ID, number of rows in this partition)
    */
  private[converters] def perTypedDataPartition[T<:Product]()
                                                           (keyName: String, vecTypes: Array[Byte], uploadPlan: Option[UploadPlan], writeTimeout: Int)
                                                           (context: TaskContext, it: Iterator[T]): (Int, Long) = {
    val (iterator, dataSize) = WriteConverterCtxUtils.bufferedIteratorWithSize(uploadPlan, it)
    // An array of H2O NewChunks; A place to record all the data in this partition
    val con = WriteConverterCtxUtils.create(uploadPlan, context.partitionId(), dataSize, writeTimeout)

    con.createChunks(keyName, vecTypes, context.partitionId(), Array.empty[Int])
    iterator.foreach(prod => { // For all rows which are subtype of Product
      for ( i <- 0 until prod.productArity ) { // For all fields...
      val fld = prod.productElement(i)
        val x = fld match {
          case Some(n) => n
          case _ => fld
        }
        con.putAnySupportedType(i, x)
      }
    })
    //Compress & write data in partitions to H2O Chunks
    con.closeChunks()

    // Return Partition number and number of rows in this partition
    (context.partitionId, con.numOfRows())
  }
}
