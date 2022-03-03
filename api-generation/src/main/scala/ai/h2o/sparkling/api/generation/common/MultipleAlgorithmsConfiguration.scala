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

package ai.h2o.sparkling.api.generation.common

/**
  * It's expected that this configuration source describes several (simple) algorithms
  * where each algorithm has just one relevant parameter in parametersConfiguration (sequence)
  */
trait MultipleAlgorithmsConfiguration extends ConfigurationSource {

  override def algorithmParametersPairs: Seq[(AlgorithmSubstitutionContext, Seq[ParameterSubstitutionContext])] = {
    algorithmConfiguration.zip(parametersConfiguration).map { case (alg, par) => (alg, Seq(par)) }
  }

  override def specificAlgorithmParametersPairs
      : Seq[(ProblemSpecificAlgorithmSubstitutionContext, Seq[ParameterSubstitutionContext])] = {

    problemSpecificAlgorithmConfiguration.zip(parametersConfiguration).map { case (alg, par) => (alg, Seq(par)) }
  }
}
