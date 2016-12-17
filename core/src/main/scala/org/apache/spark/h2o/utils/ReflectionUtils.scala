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

import org.apache.spark.h2o.utils.SupportedTypes._
import org.apache.spark.sql.types._
import water.api.API
import water.fvec.Vec

import scala.language.postfixOps
import scala.reflect.ClassTag
import scala.reflect.runtime.universe._

/**
 * Work with reflection only inside this helper.
 */
object ReflectionUtils {
  type NameOfType = String

  def fieldNamesOf(t: Type) : Array[String] = {
    t.members.sorted.collect { case m if !m.isMethod => m.name.toString.trim }.toArray
  }

  def fieldNamesOf[T: TypeTag] : Array[String] = fieldNamesOf(typeOf[T])

  def vecTypesOf[T:TypeTag]: Array[VecType] = memberTypesOf[T] map (_.vecType)

  def memberTypesOf[T](implicit ttag: TypeTag[T]): Array[SupportedType] =  {
    val types: Seq[Type] = listMemberTypes(typeOf[T])
    types map supportedTypeFor toArray
  }

  def types(st: Type): Array[Class[_]] = {
    val tt = listMemberTypes(st)
    tt map (supportedTypeFor(_).javaClass) toArray
  }

  // TODO(vlad): get rid of this duplication (see productMembers)
  def listMemberTypes(st: Type): Seq[Type] = {
    val names = fieldNamesOf(st)
    val formalTypeArgs = st.typeSymbol.asClass.typeParams
    val TypeRef(_, _, actualTypeArgs) = st
    val attr = st.members.sorted
      .filter(!_.isMethod)
      .filter( s => names.contains(s.name.toString.trim))
      .map( s =>
        s.typeSignature.substituteTypes(formalTypeArgs, actualTypeArgs)
      )
    attr
  }

  def productMembers[T:TypeTag]: Array[ProductMember] = {
    val st = typeOf[T]
    val formalTypeArgs = st.typeSymbol.asClass.typeParams
    val TypeRef(_, _, actualTypeArgs) = st
    val attr = st.members.sorted
      .filter(!_.isMethod)
        .map(s => (s.name.toString.trim, s))
      .map { p =>
        val supportedType = supportedTypeFor(p._2.typeSignature.substituteTypes(formalTypeArgs, actualTypeArgs))
        ProductMember(p._1, supportedType.toString, supportedType.javaClass)
      }
    attr toArray
  }

  def reflector(ref: AnyRef) = new {
    def getV[T](name: String): T = ref.getClass.getMethods.find(_.getName == name).get.invoke(ref).asInstanceOf[T]
    def setV(name: String, value: Any): Unit = ref.getClass.getMethods.find(_.getName == name + "_$eq").get.invoke(ref, value.asInstanceOf[AnyRef])
  }

  /** Return API annotation assigned with the given field
    * or null.
    *
    * @param klazz  class to query
    * @param fieldName  field name to query
    * @return instance of API annotation assigned with the field or null
    */
  def api(klazz: Class[_], fieldName: String): API = {
    klazz.getField(fieldName).getAnnotation(classOf[API])
  }

  import scala.reflect.runtime.universe._

  def supportedTypeOf(value : Any): SupportedType = {
    value match {
      case n: Byte => Byte
      case n: Short => Short
      case n: Int => Integer
      case n: Long => Long
      case n: Float => Float
      case n: Double => Double
      case n: Boolean => Boolean
      case n: String => String
      case n: java.sql.Timestamp => Timestamp
      case n: DataType => bySparkType(n)
      case q => throw new IllegalArgumentException(s"Do not understand type $q")
    }
  }

  def javaClassOf[T](implicit ttag: TypeTag[T]) = supportedTypeFor(typeOf[T]).javaClass

  def supportedTypeFor(tpe: Type) : SupportedType = SupportedTypes.byType(tpe)

  def classFor(tpe: Type) : Class[_] = supportedTypeFor(tpe).javaClass

  def vecTypeFor(t: Class[_]): Byte = byClass(t).vecType

  def vecTypeFor(t: Type): Byte = vecTypeFor(classFor(t))

  def vecTypeOf[T](implicit ttag: TypeTag[T]) = vecTypeFor(typeOf[T])

  /** Method translating SQL types into Sparkling Water types */
  def vecTypeFor(dt : DataType) : Byte = bySparkType(dt).vecType

  import SupportedTypes._

  /**
    * Return catalyst structural type for given H2O vector.
    *
    * The mapping of type is flat.
    *
    * @param v H2O vector
    * @return catalyst data type
    * @throws IllegalArgumentException if type is recognized
    */
  def dataTypeFor(v: Vec): DataType = supportedType(v).sparkType

  def memberTypes(p: Product) = p.productIterator map supportedTypeOf toArray

  def supportedType(v: Vec): SupportedType = {
    v.get_type() match {
      case Vec.T_BAD  => Byte // vector is full of NAs, use any type
      case Vec.T_NUM  => detectSupportedNumericType(v)
      case Vec.T_CAT  => String
      case Vec.T_UUID => String
      case Vec.T_STR  => String
      case Vec.T_TIME => Timestamp
      case typ => throw new IllegalArgumentException("Unknown vector type " + typ)
    }
  }

  private def detectSupportedNumericType(v: Vec): SupportedType = {
    if (v.isInt) {
      val min = v.min()
      val max = v.max()
      if (min > scala.Byte.MinValue && max < scala.Byte.MaxValue) {
        Byte
      } else if (min > scala.Short.MinValue && max < scala.Short.MaxValue) {
        Short
      } else if (min > scala.Int.MinValue && max < scala.Int.MaxValue) {
        Integer
      } else {
        Long
      }
    } else Double
  }
}

import ReflectionUtils._

case class ProductMember(name: String, typeName: NameOfType, typeClass: Class[_]) {
  override def toString = s"$name: $typeName"
}

case class ProductType(members: Array[ProductMember]) {
  lazy val memberNames = members map (_.name)
  // We keep names, because of a Scala 10.1 bug that does not allow to serialize TypeTags.
  lazy val memberTypeNames = members map (_.typeName)
  lazy val memberClasses = members map (_.typeClass)

  def arity = members.length

  def isSingleton = arity == 1

  override def toString = s"ProductType(${members mkString ","})"
}

object ProductType {
  def create[A <: Product: TypeTag: ClassTag]: ProductType = {
    val members = productMembers[A]
    new ProductType(members)
  }
}
