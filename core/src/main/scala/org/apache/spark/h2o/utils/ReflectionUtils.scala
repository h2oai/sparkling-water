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

import water.api.API

/**
 * Work with reflection only inside this helper.
 */
object ReflectionUtils {
  import scala.reflect.runtime.universe._

  def names[T: TypeTag] : Array[String] = {
    val tt = typeOf[T].members.sorted.filter(!_.isMethod).toArray
    tt.map(_.name.toString.trim)
  }

  def types[T: TypeTag](filter: Array[String]) : Array[Class[_]] = types(typeOf[T], filter)

  def types[T: TypeTag] : Array[Class[_]] = types(typeOf[T], new Array[String](0))

  def types(st: `Type`, nameFilter: Array[String]): Array[Class[_]] = {
    val attr = listMemberTypes(st, nameFilter)
    types(attr)
  }

  def listMemberTypes(st: `Type`, nameFilter: Array[String]): Seq[`Type`] = {
    val formalTypeArgs = st.typeSymbol.asClass.typeParams
    val TypeRef(_, _, actualTypeArgs) = st
    val attr = st.members.sorted
      .filter(!_.isMethod)
      .filter( s => nameFilter.contains(s.name.toString.trim))
      .map( s =>
        s.typeSignature.substituteTypes(formalTypeArgs, actualTypeArgs)
      )
    attr
  }

  def nameType(t: `Type`): String = {
    val name = typ(t).getSimpleName
    if (t <:< typeOf[Option[_]]) s"Option[$name]" else name
  }

  def typeNames[T: TypeTag](nameFilter: Array[String]): Seq[String] = {
    val st: `Type` = typeOf[T]
    val attr = listMemberTypes(st, nameFilter)
    typeNames(attr)
  }

  def typeNames(tt: Seq[`Type`]) : Seq[String] = {
    tt map nameType
  }

  def types(tt: Seq[`Type`]) : Array[Class[_]] = {
    (tt map typ).toArray
  }

  def typ(tpe: `Type`) : Class[_] = {
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
}
