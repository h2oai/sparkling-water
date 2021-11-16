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

package ai.h2o.sparkling.ml.metrics

import ai.h2o.sparkling.ml.models.H2OGAMMOJOModel
import hex.ModelMetrics.IndependentMetricBuilder
import hex.MultinomialAucType
import hex.gam.IndependentMetricBuilderGAM
import hex.genmodel.easy.EasyPredictModelWrapper
import hex.glm.GLMModel.GLMWeightsFun
import hex.glm.GLMModel

trait GAMMetricCalculation {
  self: H2OGAMMOJOModel =>

  override private[sparkling] def makeMetricBuilder(wrapper: EasyPredictModelWrapper): IndependentMetricBuilder[_] = {
    val family = GLMModel.GLMParameters.Family.valueOf(getFamily())
    val link = GLMModel.GLMParameters.Link.valueOf(getLink())
    val variancePower = getTweedieVariancePower()
    val linkPower = getTweedieLinkPower()
    val theta = getTheta()
    val glmf = new GLMWeightsFun(family, link, variancePower, linkPower, theta)
    val aucType = MultinomialAucType.valueOf(getAucType())
    val intercept = getIntercept()

    val responseColumn = wrapper.m._responseColumn
    val nClasses = wrapper.m.nclasses()
    val responseDomain = wrapper.m.getDomainValues(responseColumn)
    val ymu = null // TODO
    val rank = 0 // TODO

    new IndependentMetricBuilderGAM(responseDomain, ymu, glmf, rank, true, intercept, nClasses, aucType)
  }
}
