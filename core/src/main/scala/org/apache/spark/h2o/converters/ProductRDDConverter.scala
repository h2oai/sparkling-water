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
import org.apache.spark.h2o.backends.external.ExternalBackendUtils
import org.apache.spark.h2o.utils.ReflectionUtils
import org.apache.spark.internal.Logging
import water.Key

import scala.language.{implicitConversions, postfixOps}
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

private[h2o] object ProductRDDConverter extends Logging {

  /** Transform H2OFrame to Product RDD */
  def toRDD[A <: Product : TypeTag : ClassTag, T <: Frame](hc: H2OContext, fr: T): H2ORDD[A, T] = {
    new H2ORDD[A, T](fr)(hc)
  }

  /** Transform RDD to H2OFrame. This method expects RDD of type Product without TypeTag */
  def toH2OFrame(hc: H2OContext, rdd: RDD[Product], frameKeyName: Option[String]): H2OFrame = {
    H2OFrameFromRDDProductBuilder(hc, rdd, frameKeyName).withDefaultFieldNames()
  }

  /** Transform typed RDD into H2O Frame */
  def toH2OFrame[T <: Product : ClassTag: TypeTag](hc: H2OContext, rdd: RDD[T], frameKeyName: Option[String]): H2OFrame = {
    val keyName = frameKeyName.getOrElse("frame_rdd_" + rdd.id + Key.rand()) // There are uniq IDs for RDD

    val fnames = ReflectionUtils.fieldNamesOf[T]
    val ftypes = ReflectionUtils.types(typeOf[T])

    // Collect H2O vector types for all input types
    val vecTypes = ftypes.map(ReflectionUtils.vecTypeFor)

    // in case of internal backend, store regular vector types
    // otherwise for external backend store expected types
    val expectedTypes = if (hc.getConf.runsInInternalClusterMode) {
      vecTypes
    } else {
      ExternalBackendUtils.prepareExpectedTypes(ftypes)
    }

    WriteConverterCtxUtils.convert[T](hc, rdd, keyName, fnames, expectedTypes, Array.empty[Int], H2OFrameFromRDDProductBuilder.perTypedDataPartition())
  }
}



