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

import org.apache.spark.h2o.utils.ReflectionUtils.NameOfType
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String
import water.fvec.Vec

import scala.language.{implicitConversions, postfixOps}
import scala.reflect.runtime.universe._

/**
  * All type associations are gathered in this file.
  */
object SupportedTypes extends Enumeration {

  type VecType = Byte

  trait SupportedType {
    def vecType: VecType
    def sparkType: DataType
    def javaClass: Class[_]

    val matches: Type => Boolean

    def name: NameOfType = toString
  }

  implicit val mirror = runtimeMirror(getClass.getClassLoader)
  def typeForClass[T](clazz: Class[T])(implicit runtimeMirror: Mirror) = runtimeMirror.classSymbol(clazz).toType
  
  final case class SimpleType[T: TypeTag](
                                           vecType  : VecType,
                                           sparkType: DataType,
                                           javaClass: Class[_], // note, not always T, since T is scala class
                                           ifMissing: String => T,
                                           private val extraTypes : Type*
      ) extends Val with SupportedType {
    val matches = (tpe : Type) => (extraTypes.toSet + typeForClass(javaClass)).exists(_ =:= tpe)
  }

  final case class OptionalType[T: TypeTag](contentType: SimpleType[T]) extends SupportedType {
    override def vecType: VecType = contentType.vecType
    override def sparkType: DataType = contentType.sparkType
    override def javaClass: Class[_] = contentType.javaClass
    override val matches: Type => Boolean = {
      case TypeRef(_, _, Seq(innerType)) => contentType matches innerType
      case _ => false
    }
    override def toString = s"Option[$contentType]"
  }

  import java.{lang => jl, sql => js}

  private val ZeroTime = new js.Timestamp(0L)

  private def onNAthrow(what: String) = {
    throw new IllegalArgumentException(s"$what value missing")
  }
  private def onNAreturn[T](value: T)(what: String) = value

  val Boolean   = SimpleType[scala.Boolean] (Vec.T_NUM,  BooleanType,   classOf[jl.Boolean  ], onNAreturn(false), typeOf[Boolean])
  val Byte      = SimpleType[scala.Byte   ] (Vec.T_NUM,  ByteType,      classOf[jl.Byte     ], onNAthrow, typeOf[Byte])
  val Short     = SimpleType[scala.Short  ] (Vec.T_NUM,  ShortType,     classOf[jl.Short    ], onNAthrow, typeOf[Short])
  val Integer   = SimpleType[scala.Int    ] (Vec.T_NUM,  IntegerType,   classOf[jl.Integer  ], onNAthrow, typeOf[Int])
  val Long      = SimpleType[scala.Long   ] (Vec.T_NUM,  LongType,      classOf[jl.Long     ], onNAthrow, typeOf[Long])
  val Float     = SimpleType[scala.Float  ] (Vec.T_NUM,  FloatType,     classOf[jl.Float    ], onNAreturn(scala.Float.NaN), typeOf[Float])
  val Double    = SimpleType[scala.Double ] (Vec.T_NUM,  DoubleType,    classOf[jl.Double   ], onNAreturn(scala.Double.NaN), typeOf[Double])
  val Timestamp = SimpleType[js.Timestamp ] (Vec.T_TIME, TimestampType, classOf[js.Timestamp], onNAthrow)
  val Date      = SimpleType[js.Date      ] (Vec.T_TIME, DateType,      classOf[js.Date     ], onNAthrow)
  val String    = SimpleType[String       ] (Vec.T_STR,  StringType,    classOf[String],       onNAreturn(null), typeOf[String])
  // TODO(vlad): figure it out, why
  val UTF8      = SimpleType[UTF8String   ] (Vec.T_STR,  StringType,    classOf[String],       onNAreturn(null), typeOf[UTF8String])

  private implicit def val2type(v: Value): SimpleType[_] = v.asInstanceOf[SimpleType[_]]

  val allSimple: List[SimpleType[_]] = values.toList map val2type

  val allOptional: List[OptionalType[_]] = allSimple map (t => OptionalType(t))

  val all: List[SupportedType] = allSimple ++ allOptional

  private def index[F, T <: SupportedType](what: List[T]) =  new {
    def by(f: T => F): Map[F, T] = what map (t => f(t) -> t) toMap
  }

  val byClass:      Map[Class[_],      SimpleType[_]]   = index (allSimple)   by (_.javaClass)
  val byVecType:    Map[VecType,       SimpleType[_]]   = index (allSimple)   by (_.vecType)
  val bySparkType:  Map[DataType,      SimpleType[_]]   = index (allSimple)   by (_.sparkType)
  val simpleByName: Map[NameOfType,    SimpleType[_]]   = index (allSimple)   by (_.name)
  val byName:       Map[NameOfType,    SupportedType]   = index (all)         by (_.name)
  val byBaseType:   Map[SimpleType[_], OptionalType[_]] = index (allOptional) by (_.contentType)

  def byType(tpe: Type) : SupportedType = {
    SupportedTypes.all find (_.matches(tpe)) getOrElse {
      throw new IllegalArgumentException(s"Type $tpe is not supported!")
    }
  }
}
