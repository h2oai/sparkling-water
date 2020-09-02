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

import java.util

import ai.h2o.sparkling.H2OFrame
import hex.KeyValue

import scala.collection.JavaConverters._

trait HasMonotoneConstraints extends H2OAlgoParamsBase {
  private val monotoneConstraints = new DictionaryParam(
    this,
    "monotoneConstraints",
    "A key must correspond to a feature name and value could be 1 or -1")

  setDefault(monotoneConstraints -> new util.HashMap[String, Double]())

  def getMonotoneConstraints(): Map[String, Double] = $(monotoneConstraints).asScala.toMap

  def setMonotoneConstraints(value: Map[String, Double]): this.type = set(monotoneConstraints, value.asJava)

  override private[sparkling] def getH2OAlgorithmParams(trainingFrame: H2OFrame): Map[String, Any] = {
    super.getH2OAlgorithmParams(trainingFrame) ++ Map("monotone_constraints" -> getMonotoneConstraints())
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++ Map("monotoneConstraints" -> "monotone_constraints")
  }
}
