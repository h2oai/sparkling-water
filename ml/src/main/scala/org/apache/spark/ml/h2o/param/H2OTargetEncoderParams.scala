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

import org.apache.spark.ml.h2o.features._
import org.apache.spark.ml.param._

trait H2OTargetEncoderParams extends Params {

  //
  // List of Parameters
  //
  protected final val foldCol = new NullableStringParam(this, "foldCol", "Fold column name")
  protected final val labelCol = new Param[String](this, "labelCol", "Label column name")
  protected final val inputCols = new StringArrayParam(this, "inputCols", "Names of columns that will be transformed")
  protected final val holdoutStrategy = new H2OTargetEncoderHoldoutStrategyParam(this,
    "holdoutStrategy",
    """A strategy deciding what records will be excluded when calculating the target average on the training dataset.
      |Options:
      | None        - All rows are considered for the calculation
      | LeaveOneOut - All rows except the row the calculation is made for
      | KFold       - Only out-of-fold data is considered (The option requires foldCol to be set.)
    """.stripMargin)
  protected final val blendedAvgEnabled = new BooleanParam(this,
    "blendedAvgEnabled",
    """If set, the target average becomes a weighted average of the posterior average for a given categorical level and the prior average of the target.
      |The weight is determined by the size of the given group that the row belongs to. By default, the blended average is disabled.""".stripMargin)
  protected final val blendedAvgInflectionPoint = new DoubleParam(this,
    "blendedAvgInflectionPoint",
    """A parameter of the blended average. The bigger number is set, the groups relatively bigger to the overall data set size will consider
      | the global target value as a component in the weighted average. The default value is 10.""".stripMargin)
  protected final val blendedAvgSmoothing = new DoubleParam(this,
    "blendedAvgSmoothing",
    "A parameter of blended average. Controls the rate of transition between a group target value and a global target value. The default value is 20.")
  protected final val noise = new DoubleParam(this,
    "noise",
    """Amount of random noise added to output values of training dataset.
      |The default value is 0.01. Noise addition can be disabled by setting the parameter to 0.0""".stripMargin)
  protected final val noiseSeed = new LongParam(this, "noiseSeed", "A seed of the generator producing the random noise")

  //
  // Default values
  //
  setDefault(
    foldCol -> null,
    labelCol -> "label",
    inputCols -> Array[String](),
    holdoutStrategy -> H2OTargetEncoderHoldoutStrategy.None,
    blendedAvgEnabled -> false,
    blendedAvgInflectionPoint -> 10.0,
    blendedAvgSmoothing -> 20.0,
    noise -> 0.01,
    noiseSeed -> -1
  )

  //
  // Getters
  //
  def getFoldCol(): String = $(foldCol)

  def getLabelCol(): String = $(labelCol)

  def getInputCols(): Array[String] = $(inputCols)

  def getOutputCols(): Array[String] = getInputCols().map(_ + "_te")

  def getHoldoutStrategy(): H2OTargetEncoderHoldoutStrategy = $(holdoutStrategy)

  def getBlendedAvgEnabled(): Boolean = $(blendedAvgEnabled)

  def getBlendedAvgInflectionPoint(): Double = $(blendedAvgInflectionPoint)

  def getBlendedAvgSmoothing(): Double = $(blendedAvgSmoothing)

  def getNoise(): Double = $(noise)

  def getNoiseSeed(): Long = $(noiseSeed)
}

class H2OTargetEncoderHoldoutStrategyParam private[h2o](
    parent: Params,
    name: String,
    doc: String,
    isValid: H2OTargetEncoderHoldoutStrategy => Boolean)
  extends EnumParam[H2OTargetEncoderHoldoutStrategy](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)
}
