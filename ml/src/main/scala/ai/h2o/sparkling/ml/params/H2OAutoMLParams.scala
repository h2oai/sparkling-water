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
package ai.h2o.sparkling.ml.params

trait H2OAutoMLParams
  extends H2OCommonParams
  with H2OAutoMLBuildControlParams
  with H2OAutoMLBuildModelsParams
  with H2OAutoMLInputParams
  with H2OAutoMLStoppingCriteriaParams
  with HasMonotoneConstraints {
  override private[sparkling] def getExcludedCols(): Seq[String] = {
    Seq(getLabelCol(), getFoldCol(), getWeightCol())
      .flatMap(Option(_)) // Remove nulls
  }
}
