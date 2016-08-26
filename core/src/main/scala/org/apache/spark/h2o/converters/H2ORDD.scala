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


import org.apache.spark.h2o.H2OConf
import org.apache.spark.h2o.utils.ReflectionUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, SparkContext, TaskContext}
import water.fvec.Frame

import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
  * Convert H2OFrame into an RDD (lazily).
  * @param frame  an instance of H2O frame
  * @param colNames names of columns
  * @param sc  an instance of Spark context
  * @tparam A  type for resulting RDD
  * @tparam T  specific type of H2O frame
  */
private[spark]
class H2ORDD[A <: Product: TypeTag: ClassTag, T <: Frame] private(@transient val frame: T,
                                                                  val colNames: Array[String])
                                                                 (@transient sc: SparkContext)
  extends {
    override val isExternalBackend = H2OConf(sc).runsInExternalClusterMode
  } with RDD[A](sc, Nil) with H2ORDDLike[T] {

  // Get column names before building an RDD
  def this(@transient fr : T)
          (@transient sc: SparkContext) = this(fr, ReflectionUtils.names[A])(sc)

  // Check that H2OFrame & given Scala type are compatible
  if (colNames.length > 1) {
    colNames.foreach { name =>
      if (frame.find(name) == -1) {
        throw new IllegalArgumentException("Scala type has field " + name +
          " but H2OFrame does not have a matching column; has " + frame.names().mkString(","))
      }
    }
  }

  /** Number of columns in the full dataset */
  val numCols = frame.numCols()
  val types = ReflectionUtils.types[A](colNames)
  val expectedTypesAll: Option[Array[Byte]] = ConverterUtils.prepareExpectedTypes(isExternalBackend, types)

  /**
   * :: DeveloperApi ::
   * Implemented by subclasses to compute a given partition.
   */
  override def compute(split: Partition, context: TaskContext): Iterator[A] = {

    val iterator = new H2OChunkIterator[A] {

      val jc = implicitly[ClassTag[A]].runtimeClass
      val cs = jc.getConstructors
      val ccr = cs.collectFirst({
                case c if c.getParameterTypes.length == colNames.length => c
              })
        .getOrElse({
            throw new IllegalArgumentException(
                  s"Constructor must take exactly ${colNames.length} args")
      })

      override val keyName = frameKeyName
      override val partIndex = split.index
      val expectedTypes: Option[Array[Byte]] = expectedTypesAll

      /* Converter context */
      override val converterCtx: ReadConverterContext =
      ConverterUtils.getReadConverterContext(isExternalBackend,
        keyName,
        chksLocation,
        expectedTypes,
        partIndex)

      def next(): A = {
        val data = new Array[Option[Any]](numCols)
        // FIXME: this is not perfect since ncols does not need to match number of names
        (0 until numCols).foreach{ idx =>
          data(idx) = types(idx) match {
            case q if q == classOf[Integer]           => Option(converterCtx.getInt(idx))
            case q if q == classOf[java.lang.Long]    => Option(converterCtx.getLong(idx))
            case q if q == classOf[java.lang.Double]  => Option(converterCtx.getDouble(idx))
            case q if q == classOf[java.lang.Float]   => Option(converterCtx.getFloat(idx))
            case q if q == classOf[java.lang.Boolean] => Option(converterCtx.getBoolean(idx))
            case q if q == classOf[String] => Option(converterCtx.getString(idx))
            case _ => None
          }
        }

        converterCtx.increaseRowIdx()
        // Create instance for the extracted row
        ccr.newInstance(data:_*).asInstanceOf[A]
      }
    }

    ConverterUtils.getIterator[A](isExternalBackend, iterator)
  }
}
