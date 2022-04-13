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
  * It's expected that this configuration source describes just one algorithm (e.g. AutoML, GridSearch, ...)
  * where the whole parametersConfiguration (sequence) is relevant to the algorithm
  */
trait SingleAlgorithmConfiguration extends ConfigurationSource {

  override def algorithmParametersPairs: Seq[(AlgorithmSubstitutionContext, Seq[ParameterSubstitutionContext])] = {
    Seq((algorithmConfiguration.head, parametersConfiguration))
  }

  override def specificAlgorithmParametersPairs
      : Seq[(ProblemSpecificAlgorithmSubstitutionContext, Seq[ParameterSubstitutionContext])] = {

    val algos = problemSpecificAlgorithmConfiguration

    if (algos.isEmpty) Seq.empty else Seq((algos.head, parametersConfiguration))
  }
}
