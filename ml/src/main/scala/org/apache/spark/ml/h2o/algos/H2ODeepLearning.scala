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
import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.h2o.algos.params.H2OAlgoParams
import org.apache.spark.ml.h2o.models.H2ODeepLearningModel
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.SQLContext


/**
  *  Creates H2ODeepLearning model
  *  If the key specified the training set is specified using setTrainKey, then frame with this key is used as the
  *  training frame, otherwise it uses the frame from the previous stage as the training frame
  */
class H2ODeepLearning(parameters: Option[DeepLearningParameters], override val uid: String)
                     (implicit h2oContext: H2OContext, sqlContext: SQLContext)
  extends H2OAlgorithm[DeepLearningParameters, H2ODeepLearningModel](parameters)
    with H2ODeepLearningParams {

  type SELF = H2ODeepLearning

  def this()(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(None, Identifiable.randomUID("dl"))
  def this(parameters: DeepLearningParameters)(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(Option(parameters),Identifiable.randomUID("dl"))
  def this(parameters: DeepLearningParameters, uid: String)(implicit h2oContext: H2OContext, sqlContext: SQLContext) = this(Option(parameters),uid)

  override def defaultFileName: String = H2ODeepLearning.defaultFileName

  override def trainModel(params: DeepLearningParameters): H2ODeepLearningModel = {
    val model = new DeepLearning(params).trainModel().get()
    new H2ODeepLearningModel(model)
  }

}

object H2ODeepLearning extends MLReadable[H2ODeepLearning] {

  private final val defaultFileName = "dl_params"

  @Since("1.6.0")
  override def read: MLReader[H2ODeepLearning] = new H2OAlgorithmReader[H2ODeepLearning, DeepLearningParameters](defaultFileName)

  @Since("1.6.0")
  override def load(path: String): H2ODeepLearning = super.load(path)
}
/**
  * Parameters here can be set as normal and are duplicated to DeepLearningParameters H2O object
  */
trait H2ODeepLearningParams extends H2OAlgoParams[DeepLearningParameters] {
  self: H2OAlgorithm[DeepLearningParameters, H2ODeepLearningModel] =>

  type H2O_SCHEMA = DeepLearningParametersV3

  protected def paramTag = reflect.classTag[DeepLearningParameters]
  protected def schemaTag = reflect.classTag[H2O_SCHEMA]

  /** @group setParam */
  def setEpochs(value: Double) = set(epochs, value){getParams._epochs = value}

  /** @group setParam */
  def setL1(value: Double) = set(l1, value){getParams._l1 = value}

  /** @group setParam */
  def setL2(value: Double) = set(l2, value){getParams._l2 = value}

  /** @group setParam */
  def setHidden(value: Array[Int]) = set(hidden, value){getParams._hidden = value}

  /** @group setParam */
  def setResponseColumn(value: String) = set(responseColumn,value){getParams._response_column = value}

  /**
    * All parameters should be set here along with their documentation and explained default values
    */
  private final val epochs = doubleParam("epochs")
  private final val l1 = doubleParam("l1")
  private final val l2 = doubleParam("l2")
  private final val hidden = new IntArrayParam(this, "hidden", doc("hidden"))
  private final val responseColumn = param[String]("responseColumn")

  setDefault(
    epochs -> parameters._epochs,
    l1 -> parameters._l1,
    l2 -> parameters._l2,
    hidden -> parameters._hidden,
    responseColumn -> parameters._response_column)

  /** @group getParam */
  def getEpochs: Double = $(epochs)
  /** @group getParam */
  def getL1: Double = $(l1)
  /** @group getParam */
  def getL2: Double = $(l2)
  /** @group getParam */
  def getHidden: Array[Int] = $(hidden)
  /** @group getParam */
  def getResponseColumn: String = $(responseColumn)

}
