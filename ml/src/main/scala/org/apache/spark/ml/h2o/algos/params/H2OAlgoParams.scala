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
package org.apache.spark.ml.h2o.algos.params

import com.google.common.base.CaseFormat
import hex.Model.Parameters
import org.apache.spark.h2o.utils.ReflectionUtils._
import org.apache.spark.ml.h2o.algos.H2OAlgorithm
import org.apache.spark.ml.param._
import org.apache.spark.ml.param.h2o.H2OKeyParam
import water.fvec.Frame

import scala.reflect.ClassTag

/**
  * A trait extracting a shared parameters among all models.
  */
trait H2OAlgoParams[P <: Parameters] extends Params {
  // Target schema type
  type H2O_SCHEMA

  type SELF
  // Class tag for parameters to get runtime class
  protected def paramTag: ClassTag[P]

  // The same for schema
  protected def schemaTag: ClassTag[H2O_SCHEMA]

  protected var parameters = paramTag.runtimeClass.newInstance().asInstanceOf[P]

  def getParams: P = parameters

  def setParams(params: P) = this.parameters = params

  protected def doc(fieldName: String) = api(schemaTag.runtimeClass, fieldName).help()


  def booleanParam(name: String): BooleanParam = {
    val underscoredName = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name)
    new BooleanParam(this, name, doc(underscoredName))
  }

  def intParam(name: String): IntParam = {
    val underscoredName = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name)
    new IntParam(this, name, doc(underscoredName))
  }

  def longParam(name: String): LongParam = {
    val underscoredName = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name)
    new LongParam(this, name, doc(underscoredName))
  }

  def doubleParam(name: String): DoubleParam = {
    val underscoredName = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name)
    new DoubleParam(this, name, doc(underscoredName))
  }

  def param[T](name: String): Param[T] = {
    val underscoredName = CaseFormat.LOWER_CAMEL.to(CaseFormat.LOWER_UNDERSCORE, name)
    new Param[T](this, name, doc(underscoredName))
  }

  /**
    * By default it is set to 1.0 which use whole frame for training
    */
  final val ratio = new DoubleParam(this, "ratio", "Determines in which ratios split the dataset")
  final val predictionCol: Param[String] = new Param[String](this, "predictionCol", "Prediction column name")
  final val featuresCols: StringArrayParam = new StringArrayParam(this, "featuresCols", "Name of feature columns")
  setDefault(ratio -> 1.0)
  setDefault(predictionCol -> "prediction")
  setDefault(featuresCols -> Array.empty[String])

  /** @group getParam */
  def getTrainRatio: Double = $(ratio)

  /** @group setParam */
  def setTrainRatio(value: Double) = set(ratio, value) {}

  /** @group getParam */
  def getPredictionsCol: String = $(predictionCol)

  /** @group setParam */
  def setPredictionsCol(value: String) = set(predictionCol, value) {
    getParams._response_column = value
  }

  /** @group getParam */
  final def getFeaturesCols: Array[String] = $(featuresCols)

  /** @group setParam */
  def setFeaturesCols(first: String, others: String*) = set(featuresCols, Array(first) ++ others) {}

  /** @group setParam */
  def setFeaturesCols(cols: Array[String]) = {
    if (cols.length == 0) {
      throw new IllegalArgumentException("Array with feature columns must contain at least one column")
    }
    set(featuresCols, cols) {}
  }


  def setFeaturesCol(first: String) = setFeaturesCols(first)

  /**
    * Set the param and execute custom piece of code
    */
  protected final def set[T](param: Param[T], value: T)(f: => Unit): SELF = {
    f
    super.set(param, value).asInstanceOf[SELF]
  }
}
