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

package ai.h2o.sparkling.ml.algos

import hex.genmodel.utils.DistributionFamily
import hex.glm.GLMModel.GLMParameters.Family

object ProblemType extends Enumeration {
  type ProblemType = Value
  val Classification, Regression, Both = Value

  def familyToProblemType(family: String): ProblemType = {
    val enumValue = Family.valueOf(family)
    enumValue match {
      case Family.AUTO => Both
      case Family.gaussian => Regression
      case Family.binomial => Classification
      case Family.fractionalbinomial => Regression
      case Family.ordinal => Classification
      case Family.quasibinomial => Regression
      case Family.multinomial => Classification
      case Family.poisson => Regression
      case Family.gamma => Regression
      case Family.tweedie => Regression
      case Family.negativebinomial => Regression
    }
  }

  def distributionToProblemType(distribution: String): ProblemType = {
    val enumValue = DistributionFamily.valueOf(distribution)
    enumValue match {
      case DistributionFamily.AUTO => Both
      case DistributionFamily.bernoulli => Classification
      case DistributionFamily.quasibinomial => Regression
      case DistributionFamily.multinomial => Classification
      case DistributionFamily.gaussian => Regression
      case DistributionFamily.poisson => Regression
      case DistributionFamily.gamma => Regression
      case DistributionFamily.laplace => Regression
      case DistributionFamily.quantile => Regression
      case DistributionFamily.huber => Regression
      case DistributionFamily.tweedie => Regression
      case DistributionFamily.ordinal => Classification
      case DistributionFamily.modified_huber => Classification
      case DistributionFamily.negativebinomial => Regression
      case DistributionFamily.fractionalbinomial => Regression
      case DistributionFamily.custom => Both
    }
  }
}
