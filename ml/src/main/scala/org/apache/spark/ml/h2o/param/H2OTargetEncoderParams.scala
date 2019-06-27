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
import org.apache.spark.ml.param.{Param, Params, StringArrayParam}

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
  protected final val blending = new CaseClassParam[H2OTargetEncoderBlendingSettings](this,
    "blending",
    """If set, the target average becomes a weighted average of the posterior average for a given categorical level and the prior average of the target.
      |The weight is determined by the size of the given group that the row belongs to.
      |Attributes:
      | InflectionPoint - The bigger number it's, the groups relatively bigger to the overall data set size will consider
      |                   the global target value as a component in the weighted average.
      | Smoothing       - Controls the rate of transition between a group target value and a global target value.""".stripMargin)
  protected final val noise = new CaseClassParam[H2OTargetEncoderNoiseSettings](this,
    "noise",
    """If set, output valus will be modified by randomly generated noise.
      |Attributes:
      | Amount - amount of random noise added to output values
      | Seed   - a seed of the generator producing the random noise
    """.stripMargin)

  //
  // Default values
  //
  setDefault(
    foldCol -> null,
    labelCol -> "label",
    inputCols -> Array[String](),
    holdoutStrategy -> H2OTargetEncoderHoldoutStrategy.None,
    blending -> null,
    noise -> H2OTargetEncoderNoiseSettings(0.01, -1)
  )

  //
  // Getters
  //
  def getFoldCol(): String = $(foldCol)

  def getLabelCol(): String = $(labelCol)

  def getInputCols(): Array[String] = $(inputCols)

  def getOutputCols(): Array[String] = getInputCols().map(_ + "_te")

  def getHoldoutStrategy(): H2OTargetEncoderHoldoutStrategy = $(holdoutStrategy)

  def getBlending(): H2OTargetEncoderBlendingSettings = $(blending)

  def getNoise(): H2OTargetEncoderNoiseSettings = $(noise)
}

class H2OTargetEncoderHoldoutStrategyParam private[h2o](
    parent: Params,
    name: String,
    doc: String,
    isValid: H2OTargetEncoderHoldoutStrategy => Boolean)
  extends EnumParam[H2OTargetEncoderHoldoutStrategy](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)
}
