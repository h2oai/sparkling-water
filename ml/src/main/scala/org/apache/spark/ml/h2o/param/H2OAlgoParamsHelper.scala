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
package org.apache.spark.ml.h2o.param

import com.google.common.base.CaseFormat
import hex.Model.Parameters
import org.apache.spark.h2o.utils.ReflectionUtils.api
import org.apache.spark.ml.param._

import scala.reflect.ClassTag

/**
  * Base trait providing parameters utilities and shared methods for handling parameters for H2O objects
  *
  * @tparam P H2O's parameter type
  */
trait H2OAlgoParamsHelper[P <: Parameters] extends Params {
  // Target schema type
  type H2O_SCHEMA

  // Class tag for parameters to get runtime class
  protected def paramTag: ClassTag[P]

  // The same for schema
  protected def schemaTag: ClassTag[H2O_SCHEMA]

  protected var parameters: P = paramTag.runtimeClass.newInstance().asInstanceOf[P]

  protected def booleanParam(name: String, doc: String): BooleanParam = booleanParam(name, Some(doc))

  protected def booleanParam(name: String, doc: Option[String] = None): BooleanParam = {
    new BooleanParam(this, name, getDoc(doc, name))
  }


  protected def intParam(name: String, doc: String): IntParam = intParam(name, Some(doc))

  protected def intParam(name: String, doc: Option[String] = None): IntParam = {
    new IntParam(this, name, getDoc(doc, name))
  }

  protected def longParam(name: String, doc: String): LongParam = longParam(name, Some(doc))

  protected def longParam(name: String, doc: Option[String] = None): LongParam = {
    new LongParam(this, name, getDoc(doc, name))
  }

  protected def floatParam(name: String, doc: String): FloatParam = floatParam(name, Some(doc))

  protected def floatParam(name: String, doc: Option[String] = None): FloatParam = {
    new FloatParam(this, name, getDoc(doc, name))
  }

  protected def doubleParam(name: String, doc: String): DoubleParam = doubleParam(name, Some(doc))

  protected def doubleParam(name: String, doc: Option[String] = None): DoubleParam = {
    new DoubleParam(this, name, getDoc(doc, name))
  }

  protected def param[T](name: String, doc: String): Param[T] = param[T](name, Some(doc))

  protected def param[T](name: String, doc: Option[String] = None): Param[T] = {
    new Param[T](this, name, getDoc(doc, name))
  }

  protected def stringParam(name: String, doc: String): Param[String] = stringParam(name, Some(doc))

  protected def stringParam(name: String, doc: Option[String] = None): Param[String] = {
    new Param[String](this, name, getDoc(doc, name))
  }

  protected def nullableStringParam(name: String, doc: String): NullableStringParam = {
    nullableStringParam(name, Some(doc))
  }

  protected def nullableStringParam(name: String, doc: Option[String] = None): NullableStringParam = {
    new NullableStringParam(this, name, getDoc(doc, name))
  }

  protected def stringArrayParam(name: String, doc: String): StringArrayParam = stringArrayParam(name, Some(doc))

  protected def stringArrayParam(name: String, doc: Option[String] = None): StringArrayParam = {
    new StringArrayParam(this, name, getDoc(doc, name))
  }

  protected def intArrayParam(name: String, doc: String): IntArrayParam = intArrayParam(name, Some(doc))

  protected def intArrayParam(name: String, doc: Option[String] = None): IntArrayParam = {
    new IntArrayParam(this, name, getDoc(doc, name))
  }

  protected def doubleArrayParam(name: String, doc: String): DoubleArrayParam = doubleArrayParam(name, Some(doc))

  protected def doubleArrayParam(name: String, doc: Option[String] = None): DoubleArrayParam = {
    new DoubleArrayParam(this, name, getDoc(doc, name))
  }

  protected def getH2ODoc(fieldName: String) = api(schemaTag.runtimeClass, fieldName).help()

  protected def getDoc(doc: Option[String], fieldName: String) = {
    doc.getOrElse {
      val underscoredName = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, fieldName)
      getH2ODoc(underscoredName)
    }
  }

  protected def nullableDoubleArrayParam(name: String, doc: String): NullableDoubleArrayParam = nullableDoubleArrayParam(name, Some(doc))

  protected def nullableDoubleArrayParam(name: String, doc: Option[String] = None): NullableDoubleArrayParam = {
    new NullableDoubleArrayParam(this, name, getDoc(doc, name))
  }

  protected def nullableStringArrayParam(name: String, doc: String): NullableStringArrayParam = nullableStringArrayParam(name, Some(doc))

  protected def nullableStringArrayParam(name: String, doc: Option[String] = None): NullableStringArrayParam = {
    new NullableStringArrayParam(this, name, getDoc(doc, name))
  }

  protected def checkAllowedEnumValues[T <: Enum[T]](name: String, nullAllowed: Boolean = false)
                                                    (implicit ctag: reflect.ClassTag[T]): Unit = {

    val names = ctag.runtimeClass.getDeclaredMethod("values").invoke(null).asInstanceOf[Array[T]].map(_.name())

    if (!nullAllowed && name == null) {
      throw new IllegalArgumentException(s"Null is not a valid value. Allowed values are: ${names.mkString(", ")}")
    }

    if (!names.map(_.toLowerCase()).contains(name.toLowerCase())) {
      val nullStr = if (nullAllowed) "null or " else ""
      throw new IllegalArgumentException(s"'$name' is not a valid value. Allowed values are: $nullStr${names.mkString(", ")}")
    }
  }

  // Expects that checkAllowedEnumValues method was called before this method
  protected def getCorrectEnumCase[T <: Enum[T]](name: String)
                                                (implicit ctag: reflect.ClassTag[T]): String = {
    val names = ctag.runtimeClass.getDeclaredMethod("values").invoke(null).asInstanceOf[Array[T]].map(_.name())

    if (name == null) {
      null
    } else {
      names.find(_.toLowerCase() == name.toLowerCase).get
    }
  }

}
