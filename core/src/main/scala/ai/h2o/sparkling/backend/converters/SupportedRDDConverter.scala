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

package ai.h2o.sparkling.backend.converters

import ai.h2o.sparkling.backend.H2ORDD
import org.apache.spark.h2o._
import water.DKV
import water.fvec.Frame

import scala.language.implicitConversions
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._


/**
 * This converter just wraps the existing RDD converters and hides the internal RDD converters
 */

object SupportedRDDConverter {
  /** Transform supported type for conversion to a key string of H2OFrame */
  def toH2OFrameKeyString(
                           hc: H2OContext,
                           rdd: SupportedRDD,
                           frameKeyName: Option[String]): String = {
    rdd.toH2OFrameKeyString(hc, frameKeyName)
  }

  /** Transform H2OFrame to RDD */
  def toRDD[A <: Product : TypeTag : ClassTag, T <: Frame](hc: H2OContext, fr: T): RDD[A] = {
    DKV.put(fr)
    toRDD(hc, ai.h2o.sparkling.frame.H2OFrame(fr._key.toString))
  }

  /** Transform H2OFrame to RDD */
  def toRDD[A <: Product : TypeTag : ClassTag](hc: H2OContext, fr: ai.h2o.sparkling.frame.H2OFrame): RDD[A] = {
    new H2ORDD[A](fr)(hc)
  }
}
