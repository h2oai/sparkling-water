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

package org.apache.spark.ml.h2o.algos

import hex.deeplearning.DeepLearning
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.schemas.DeepLearningV3.DeepLearningParametersV3
import org.apache.spark.annotation.Since
import org.apache.spark.ml.h2o.models.H2OMOJOModel
import org.apache.spark.ml.h2o.param.H2OAlgoParams
import org.apache.spark.ml.util._
import water.support.ModelSerializationSupport


/**
  * H2O Deep learning algorithm exposed via Spark ML pipelines.
  *
  * TODO: There are still bunch of parameters defined DeepLearningParameters which need to be ported here
  */
class H2ODeepLearning(override val uid: String) extends H2OAlgorithm[DeepLearningParameters, H2OMOJOModel]
  with H2ODeepLearningParams {

  def this() = this(Identifiable.randomUID("deeplearning"))

  override def defaultFileName: String = H2ODeepLearning.defaultFileName

  override def trainModel(params: DeepLearningParameters): H2OMOJOModel = {
    val model = new DeepLearning(params).trainModel().get()
    new H2OMOJOModel(ModelSerializationSupport.getMojoData(model))
  }

}

object H2ODeepLearning extends MLReadable[H2ODeepLearning] {

  private final val defaultFileName = "deeplearning_params"

  @Since("1.6.0")
  override def read: MLReader[H2ODeepLearning] = new H2OAlgorithmReader[H2ODeepLearning](defaultFileName)

  @Since("1.6.0")
  override def load(path: String): H2ODeepLearning = super.load(path)
}

/**
  * Parameters here can be set as normal and are duplicated to DeepLearningParameters H2O object
  */
trait H2ODeepLearningParams extends H2OAlgoParams[DeepLearningParameters] {

  type H2O_SCHEMA = DeepLearningParametersV3

  protected def paramTag = reflect.classTag[DeepLearningParameters]

  protected def schemaTag = reflect.classTag[H2O_SCHEMA]

  //
  // Param definitions
  //
  private val epochs = doubleParam("epochs")
  private val l1 = doubleParam("l1")
  private val l2 = doubleParam("l2")
  private val hidden = intArrayParam("hidden")
  private val reproducible = booleanParam("reproducible")
  //
  // Default values
  //
  setDefault(
    epochs -> parameters._epochs,
    l1 -> parameters._l1,
    l2 -> parameters._l2,
    hidden -> parameters._hidden,
    reproducible -> parameters._reproducible)

  //
  // Getters
  //
  def getEpochs(): Double = $(epochs)

  def getL1(): Double = $(l1)

  def getL2(): Double = $(l2)

  def getHidden(): Array[Int] = $(hidden)

  def getReproducible(): Boolean = $(reproducible)

  //
  // Setters
  //
  def setEpochs(value: Double): this.type = set(epochs, value)

  def setL1(value: Double): this.type = set(l1, value)

  def setL2(value: Double): this.type = set(l2, value)

  def setHidden(value: Array[Int]): this.type = set(hidden, value)

  def setReproducible(value: Boolean): this.type = set(reproducible, value)


  override def updateH2OParams(): Unit = {
    super.updateH2OParams()
    parameters._epochs = $(epochs)
    parameters._l1 = $(l1)
    parameters._l2 = $(l2)
    parameters._hidden = $(hidden)
    parameters._reproducible = $(reproducible)
  }
}
