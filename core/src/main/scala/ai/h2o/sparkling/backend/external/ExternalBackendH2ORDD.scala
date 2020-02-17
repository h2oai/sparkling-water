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

import ai.h2o.sparkling.backend.shared.H2ORDDBase
import ai.h2o.sparkling.frame.H2OFrame
import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.utils.ProductType

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
  extends H2ORDDBase[A](hc.sparkContext, hc.getConf) with ExternalBackendSparkEntity {

  // Get product type before building an RDD
  def this(@transient frame: H2OFrame)
          (@transient hc: H2OContext) = this(frame, ProductType.create[A])(hc)

  protected override val colNames = {
    val names = frame.columns.map(_.name)
    checkColumnNames(names)
    names
  }
}
