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

package org.apache.spark.rdd


import org.apache.spark.h2o.{H2OFrame, H2OContext, ReflectionUtils}
import org.apache.spark.{Partition, TaskContext}
import water.parser.BufferedString

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Convert H2OFrame into an RDD (lazily)
 */

private[spark]
class H2ORDD[A <: Product: TypeTag: ClassTag] private (@transient val h2oContext: H2OContext,
                                                       @transient val frame: H2OFrame,
                                                       val colNames: Array[String])
  extends RDD[A](h2oContext.sparkContext, Nil) with H2ORDDLike {

  // Get column names before building an RDD
  def this(h2oContext: H2OContext, fr : H2OFrame ) = this(h2oContext,fr,ReflectionUtils.names[A])
  // Check that H2OFrame & given Scala type are compatible
  if (colNames.length > 1) {
    colNames.foreach { name =>
      if (frame.find(name) == -1) {
        throw new IllegalArgumentException("Scala type has field " + name +
          " but H2OFrame does not have a matching column; has " + frame.names().mkString(","))
      }
    }
  }
  val types = ReflectionUtils.types[A](colNames)
  @transient val jc = implicitly[ClassTag[A]].runtimeClass
  @transient val cs = jc.getConstructors
  @transient val ccr = cs.collectFirst(
        { case c if (c.getParameterTypes.length==colNames.length) => c })
        .getOrElse( {
                      throw new IllegalArgumentException(
                        s"Constructor must take exactly ${colNames.length} args")})

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[A] = {
    val kn = keyName
    new H2OChunkIterator[A] {
      override val keyName = kn
      override val partIndex = split.index

      val jc = implicitly[ClassTag[A]].runtimeClass
      val cs = jc.getConstructors
      val ccr = cs.collectFirst({
                case c if (c.getParameterTypes.length==colNames.length) => c
              })
        .getOrElse({
            throw new IllegalArgumentException(
                  s"Constructor must take exactly ${colNames.length} args")
      })
      /** Dummy muttable holder for String values */
      val valStr = new BufferedString()

      def next(): A = {
        val data = new Array[Option[Any]](chks.length)
          for (
            idx <- 0 until chks.length;
            chk = chks (idx);
            typ = types(idx)
          ) {
            val value = if (chk.isNA(row)) None
            else typ match {
              case q if q == classOf[Integer]           => Some(chk.at8(row).asInstanceOf[Int])
              case q if q == classOf[java.lang.Long]    => Some(chk.at8(row))
              case q if q == classOf[java.lang.Double]  => Some(chk.atd(row))
              case q if q == classOf[java.lang.Float]   => Some(chk.atd(row))
              case q if q == classOf[java.lang.Boolean] => Some(chk.at8(row) == 1)
              case q if q == classOf[String] =>
                if (chk.vec().isCategorical) {
                  Some(chk.vec().domain()(chk.at8(row).asInstanceOf[Int]))
                } else if (chk.vec().isString) {
                  chk.atStr(valStr, row)
                  Some(valStr.toString)
                } else None
              case _ => None
            }
            data(idx) = value
          }
        row += 1
        ccr.newInstance(data:_*).asInstanceOf[A]
      }
    }
  }

  /** Pass thru an RDD if given one, else pull from the H2O Frame */
  override protected def getPartitions: Array[Partition] = {
    val num = frame.anyVec().nChunks()
    val res = new Array[Partition](num)
    for( i <- 0 until num ) res(i) = new Partition { val index = i }
    res
  }

}
