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
import org.apache.spark.internal.Logging

import scala.language.{implicitConversions, postfixOps}
import scala.reflect.runtime.universe._


private[h2o] object DatasetConverter extends Logging {

  /** Transform Spark's Dataset into H2O Frame */
  def toH2OFrame[T <: Product](hc: H2OContext, ds: Dataset[T], frameKeyName: Option[String])(implicit ttag: TypeTag[T]) = {
    val tpe = ttag.tpe
    val constructorSymbol = tpe.decl(termNames.CONSTRUCTOR)
    val defaultConstructor =
      if (constructorSymbol.isMethod) constructorSymbol.asMethod
      else {
        val ctors = constructorSymbol.asTerm.alternatives
        ctors.map { _.asMethod }.find { _.isPrimaryConstructor }.get
      }

    val params: List[(String, Type)] = defaultConstructor.paramLists.flatten map {
      sym => sym.name.toString -> tpe.member(sym.name).asMethod.returnType
    }

    val rdd: RDD[Product] = try {
      ds.rdd.asInstanceOf[RDD[Product]]
    } catch {
      case oops: Exception =>
        oops.printStackTrace()
        throw oops
    }
    val res = try {
      val prototype = H2OFrameFromRDDProductBuilder(hc, rdd, frameKeyName)
      prototype.withFields(params)
    } catch {
      case oops: Exception =>
        oops.printStackTrace()
        throw oops
    }
    res
  }
}
