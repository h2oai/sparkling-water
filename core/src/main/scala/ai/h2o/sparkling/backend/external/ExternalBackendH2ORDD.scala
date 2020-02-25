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

import ai.h2o.sparkling.backend.shared.{H2ORDDBase, Reader}
import ai.h2o.sparkling.frame.H2OFrame
import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.utils.ProductType
import org.apache.spark.rdd.RDD
import org.apache.spark.{Partition, TaskContext}

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
 */
private[backend] class ExternalBackendH2ORDD[A <: Product : TypeTag : ClassTag] private(val frame: H2OFrame, val productType: ProductType)
                                                                                       (@(transient@param @field) hc: H2OContext)
  extends RDD[A](hc.sparkContext, Nil) with H2ORDDBase[A] with ExternalBackendSparkEntity {

  override val expectedTypes: Option[Array[Byte]] = Option(ExternalH2OBackend.prepareExpectedTypes(productType.memberClasses))

  private val h2oConf = hc.getConf

  // Get product type before building an RDD
  def this(@transient frame: H2OFrame)
          (@transient hc: H2OContext) = this(frame, ProductType.create[A])(hc)

  override def compute(split: Partition, context: TaskContext): Iterator[A] = {
    new H2ORDDIterator {
      private val chnk = frame.chunks.find(_.index == split.index).head
      override val reader: Reader = new ExternalBackendReader(frameKeyName, split.index, chnk.numberOfRows,
        chnk.location, expectedTypes.get, selectedColumnIndices, h2oConf)
    }
  }

  protected override val colNames: Array[String] = {
    val names = frame.columns.map(_.name)
    checkColumnNames(names)
    names
  }

  override protected val jc: Class[_] = implicitly[ClassTag[A]].runtimeClass
}
