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
package org.apache.spark.h2o.utils

import org.apache.spark.sql.types._
import water.fvec.Vec

import scala.reflect.runtime.universe._

/**
 * Old code that is now just used for checking compatibility
 */
object OldH2OTypeUtils {

  val dataTypeToVecType = Map[Class[_], Byte](
  classOf[java.lang.Byte] -> Vec.T_NUM,
  classOf[java.lang.Short] -> Vec.T_NUM,
  classOf[java.lang.Integer] -> Vec.T_NUM,
  classOf[java.lang.Long] -> Vec.T_NUM,
  classOf[java.lang.Float] -> Vec.T_NUM,
  classOf[java.lang.Double] -> Vec.T_NUM,
  classOf[java.lang.Boolean] -> Vec.T_NUM,
  classOf[java.lang.String] -> Vec.T_STR,
  classOf[java.sql.Timestamp] -> Vec.T_TIME
  ).withDefault(q => throw new IllegalArgumentException(s"Do not understand type $q"))

  import scala.reflect.runtime.universe._

  def dataTypeToVecType(t: Type): Byte = {
    dataTypeToVecType(ReflectionUtils.classFor(t))
  }
}

object OldReflectionUtils {

  /** Method translating SQL types into Sparkling Water types */
  def dataTypeToVecType(dt : DataType) : Byte = dt match {
    case BinaryType  => Vec.T_NUM
    case ByteType    => Vec.T_NUM
    case ShortType   => Vec.T_NUM
    case IntegerType => Vec.T_NUM
    case LongType    => Vec.T_NUM
    case FloatType   => Vec.T_NUM
    case DoubleType  => Vec.T_NUM
    case BooleanType => Vec.T_NUM
    case TimestampType => Vec.T_TIME
    case StringType  => Vec.T_STR
    //case StructType  => dt.
    case _ => throw new IllegalArgumentException(s"Unsupported type $dt")
  }

  def typ(tpe: Type) : Class[_] = {
    tpe match {
      // Unroll Option[_] type
      case t if t <:< typeOf[Option[_]] =>
        val TypeRef(_, _, Seq(optType)) = t
        typ(optType)
      case t if t <:< typeOf[String]            => classOf[String]
      case t if t <:< typeOf[java.lang.Integer] => classOf[java.lang.Integer]
      case t if t <:< typeOf[java.lang.Long]    => classOf[java.lang.Long]
      case t if t <:< typeOf[java.lang.Double]  => classOf[java.lang.Double]
      case t if t <:< typeOf[java.lang.Float]   => classOf[java.lang.Float]
      case t if t <:< typeOf[java.lang.Short]   => classOf[java.lang.Short]
      case t if t <:< typeOf[java.lang.Byte]    => classOf[java.lang.Byte]
      case t if t <:< typeOf[java.lang.Boolean] => classOf[java.lang.Boolean]
      case t if t <:< definitions.IntTpe        => classOf[java.lang.Integer]
      case t if t <:< definitions.LongTpe       => classOf[java.lang.Long]
      case t if t <:< definitions.DoubleTpe     => classOf[java.lang.Double]
      case t if t <:< definitions.FloatTpe      => classOf[java.lang.Float]
      case t if t <:< definitions.ShortTpe      => classOf[java.lang.Short]
      case t if t <:< definitions.ByteTpe       => classOf[java.lang.Byte]
      case t if t <:< definitions.BooleanTpe    => classOf[java.lang.Boolean]
      case t if t <:< typeOf[java.sql.Timestamp] => classOf[java.sql.Timestamp]
      case t => throw new IllegalArgumentException(s"Type $t is not supported!")
    }
  }
}
