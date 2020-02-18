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

package ai.h2o.sparkling.backend.internal

import ai.h2o.sparkling.backend.shared.{H2ORDDBase, Reader}
import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.utils.ProductType
import org.apache.spark.{Partition, TaskContext}
import water.fvec.Frame
import water.support.H2OFrameSupport

import scala.annotation.meta.{field, param}
import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


/**
 * Convert H2OFrame into an RDD (lazily).
 *
 * @param frame       an instance of H2O frame
 * @param productType pre-calculated deconstructed type of result
 * @param hc          an instance of H2O context
 * @tparam A type for resulting RDD
 * @tparam T specific type of H2O frame
 */
private[backend] class InternalBackendH2ORDD[A <: Product : TypeTag : ClassTag, T <: Frame] private(@(transient@param @field) val frame: T,
                                                                                                    val productType: ProductType)
                                                                                                   (@(transient@param @field) hc: H2OContext)
  extends H2OAwareEmptyRDD[A](hc.sparkContext, hc.getH2ONodes(), hc.getConf) with H2ORDDBase[A] with InternalBackendSparkEntity[T] {

  // Get product type before building an RDD
  def this(@transient fr: T)
          (@transient hc: H2OContext) = this(fr, ProductType.create[A])(hc)

  override def compute(split: Partition, context: TaskContext): Iterator[A] = new H2ORDDIterator() {
    override val converterCtx: Reader = new InternalBackendReader(frameKeyName, split.index)
  }

  H2OFrameSupport.lockAndUpdate(frame)
  protected override val colNames: Array[String] = {
    val names = frame.names()
    checkColumnNames(names)
    names
  }

  override protected val jc: Class[_] = implicitly[ClassTag[A]].runtimeClass
}
