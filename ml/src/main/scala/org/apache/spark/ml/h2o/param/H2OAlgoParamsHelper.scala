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

  protected var parameters = paramTag.runtimeClass.newInstance().asInstanceOf[P]

  def getParams: P = parameters

  def setParams(params: P): Unit = this.parameters = params

  def booleanParam(name: String, doc: String): BooleanParam = booleanParam(name, Some(doc))

  def booleanParam(name: String, doc: Option[String] = None): BooleanParam = {
    new BooleanParam(this, name, getDoc(doc, name))
  }


  def intParam(name: String, doc: String): BooleanParam = booleanParam(name, Some(doc))

  def intParam(name: String, doc: Option[String] = None): IntParam = {
    new IntParam(this, name, getDoc(doc, name))
  }

  def longParam(name: String, doc: String): LongParam = longParam(name, Some(doc))

  def longParam(name: String, doc: Option[String] = None): LongParam = {
    new LongParam(this, name, getDoc(doc, name))
  }

  def doubleParam(name: String, doc: String): DoubleParam = doubleParam(name, Some(doc))

  def doubleParam(name: String, doc: Option[String] = None): DoubleParam = {
    new DoubleParam(this, name, getDoc(doc, name))
  }

  def param[T](name: String, doc: String): Param[T] = param[T](name, Some(doc))

  def param[T](name: String, doc: Option[String] = None): Param[T] = {
    new Param[T](this, name, getDoc(doc, name))
  }

  def stringParam(name: String, doc: String): Param[String] = stringParam(name, Some(doc))

  def stringParam(name: String, doc: Option[String] = None): Param[String] = {
    new Param[String](this, name, getDoc(doc, name))
  }

  def stringArrayParam(name: String, doc: String): StringArrayParam = stringArrayParam(name, Some(doc))

  def stringArrayParam(name: String, doc: Option[String] = None): StringArrayParam = {
    new StringArrayParam(this, name, getDoc(doc, name))
  }

  def intArrayParam(name: String, doc: String): IntArrayParam = intArrayParam(name, Some(doc))

  def intArrayParam(name: String, doc: Option[String] = None): IntArrayParam = {
    new IntArrayParam(this, name, getDoc(doc, name))
  }

  def doubleArrayParam(name: String, doc: String): DoubleArrayParam = doubleArrayParam(name, Some(doc))

  def doubleArrayParam(name: String, doc: Option[String] = None): DoubleArrayParam = {
    new DoubleArrayParam(this, name, getDoc(doc, name))
  }

  protected def getH2ODoc(fieldName: String) = api(schemaTag.runtimeClass, fieldName).help()

  protected def getDoc(doc: Option[String], fieldName: String) = {
    doc.getOrElse {
      val underscoredName = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, fieldName)
      getH2ODoc(underscoredName)
    }
  }
}
