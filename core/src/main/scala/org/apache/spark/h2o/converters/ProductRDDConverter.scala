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

import org.apache.spark.h2o._
import org.apache.spark.h2o.utils.H2OTypeUtils._
import org.apache.spark.h2o.utils.{H2OTypeUtils, NodeDesc, ReflectionUtils}
import org.apache.spark.{Logging, TaskContext}
import water.Key

import scala.collection.immutable
import scala.collection.mutable.ListBuffer
import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

//private[converters]
object ProductRDDConverter extends Logging with ConverterUtils{

  /** Transform H2OFrame to Product RDD */
  def toRDD[A <: Product: TypeTag: ClassTag, T <: Frame](hc: H2OContext, fr: T): RDD[A] = {
    new H2ORDD[A, T](fr)(hc.sparkContext)
  }

  /** Transform RDD to H2OFrame. This method expects RDD of type Product without TypeTag */
  def toH2OFrame(hc: H2OContext, rdd: RDD[Product], frameKeyName: Option[String]): H2OFrame = {

    val keyName = frameKeyName.getOrElse("frame_rdd_" + rdd.id + Key.rand()) // There are uniq IDs for RDD

    // infer the type
    val first = rdd.first()
    val fnames = 0.until(first.productArity).map(idx => "f" + idx).toArray[String]
    val ftypes = new ListBuffer[Class[_]]()
    val it = first.productIterator
    while(it.hasNext){
      ftypes+=inferFieldType(it.next())
    }
    // Collect H2O vector types for all input types
    val vecTypes = ftypes.toArray[Class[_]].indices.map(idx => dataTypeToVecType(ftypes(idx))).toArray

    convert[Product](hc, rdd, keyName, fnames, vecTypes, perTypedRDDPartition())
  }

  /** Transform typed RDD into H2O Frame */
  def toH2OFrame[T <: Product : TypeTag](hc: H2OContext, rdd: RDD[T], frameKeyName: Option[String]) : H2OFrame = {
    import H2OTypeUtils._
    import ReflectionUtils._

    val keyName = frameKeyName.getOrElse("frame_rdd_" + rdd.id + Key.rand()) // There are uniq IDs for RDD

    val fnames = names[T]
    val ftypes = types[T](fnames)
    // Collect H2O vector types for all input types
    val vecTypes = ftypes.indices.map(idx => dataTypeToVecType(ftypes(idx))).toArray

    convert[T](hc, rdd, keyName, fnames, vecTypes, perTypedRDDPartition())
  }

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
  private[this]
  def perTypedRDDPartition[T<:Product]()
                                      (keyName: String, vecTypes: Array[Byte], uploadPlan: Option[immutable.Map[Int, NodeDesc]])
                                      ( context: TaskContext, it: Iterator[T] ): (Int,Long) = {
    val con = ConverterUtils.getWriteConverterContext(uploadPlan, context.partitionId())
    // Creates array of H2O NewChunks; A place to record all the data in this partition
    con.createChunks(keyName, vecTypes, context.partitionId())

    it.foreach(prod => { // For all rows which are subtype of Product
      for( i <- 0 until prod.productArity ) { // For all fields...
      val fld = prod.productElement(i)
        // TODO(vlad): deal properly with Option
        val x = fld match {
          case Some(n) => n
          case _ => fld
        }
        x match {
          case n: Number  => con.put(i, n)
          case n: Boolean => con.put(i, n)
          case n: String  => con.put(i, n)
          case n : java.sql.Timestamp => con.put(i, n)
          case _ => con.putNA(i)
        }
      }
      con.increaseRowCounter()
    })

    //Compress & write data in partitions to H2O Chunks
    con.closeChunks()

    // Return Partition number and number of rows in this partition
    (context.partitionId, con.numOfRows)
  }

  /**
    * Infers the type from Any, used for determining the types in Product RDD
    *
    * @param value
    * @return
    */
  private[this] def inferFieldType(value : Any): Class[_] ={
    value match {
      case n: Byte  => classOf[java.lang.Byte]
      case n: Short => classOf[java.lang.Short]
      case n: Int => classOf[java.lang.Integer]
      case n: Long => classOf[java.lang.Long]
      case n: Float => classOf[java.lang.Float]
      case n: Double => classOf[java.lang.Double]
      case n: Boolean => classOf[java.lang.Boolean]
      case n: String => classOf[java.lang.String]
      case n: java.sql.Timestamp => classOf[java.sql.Timestamp]
      case q => throw new IllegalArgumentException(s"Do not understand type $q")
    }
  }
}
