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

package org.apache.spark.h2o

import org.apache.spark.sql.SQLContext

import scala.language.implicitConversions
import scala.reflect.runtime.universe._

  /**
  * Magnet pattern (Type Class pattern) for conversion from various case classes to their appropriate H2OFrame using
  * the method with the same name
  */
trait SupportedDataset[T] {
    val typeTag: TypeTag[T]
  def toH2OFrame(sc: SQLContext, frameKeyName: Option[String]): H2OFrame
}

object SupportedDataset {

  implicit def toH2OFrameFromDataset[T <: Product](ds: Dataset[T])(implicit ttag:TypeTag[T]): SupportedDataset[T] = new SupportedDataset[T] {
    val typeTag = ttag
    override def toH2OFrame(sc: SQLContext, frameKeyName: Option[String]): H2OFrame = {
      val tpe = ttag.tpe
      val constructorSymbol = tpe.declaration(nme.CONSTRUCTOR)
      val defaultConstructor =
        if (constructorSymbol.isMethod) constructorSymbol.asMethod
        else {
          val ctors = constructorSymbol.asTerm.alternatives
          ctors.map { _.asMethod }.find { _.isPrimaryConstructor }.get
        }

      val params: List[(String, Type)] = defaultConstructor.paramss.flatten map {
        sym => sym.name.toString -> tpe.member(sym.name).asMethod.returnType
      }

      val fieldNames = ds.schema.fieldNames

      val rdd: RDD[Product] = try {
        ds.rdd.asInstanceOf[RDD[Product]]
      } catch {
        case oops: Exception =>
          oops.printStackTrace()
          throw oops
      }
      val res = try {
        val prototype = H2OContext.FromPureProduct(sc.sparkContext, rdd, frameKeyName)
        prototype.withFields(params)
      } catch {
        case oops: Exception =>
          oops.printStackTrace()
          throw oops
      }
      res
    }
  }
}
