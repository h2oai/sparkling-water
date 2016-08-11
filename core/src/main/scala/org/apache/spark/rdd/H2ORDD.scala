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


import org.apache.spark.h2o.{H2OContext, H2OFrame, ReflectionUtils}
import org.apache.spark.{Partition, SparkContext, TaskContext}
import water.fvec.Frame
import water.parser.BufferedString

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Convert H2OFrame into an RDD (lazily)
 */

private[spark]
class H2ORDD[A <: Product: TypeTag: ClassTag, T <: Frame] private(@transient val frame: T,
                                                                  val colNames: Array[String])
                                                                 (@transient sparkContext: SparkContext)
  extends RDD[A](sparkContext, Nil) with H2ORDDLike[T] {

  // Get column names before building an RDD
  def this(@transient fr : T)
          (@transient sparkContext: SparkContext) = this(fr, ReflectionUtils.names[A])(sparkContext)

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

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[A] = {
    val kn = frameKeyName
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
}
