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

package ai.h2o.sparkling.ml.models

import ai.h2o.sparkling.ml.algos.H2OSupervisedAlgorithm
import hex.Model
import org.apache.spark.ml.param._

object ParameterSetters {
  implicit class AlgorithmWrapper(val algo: H2OSupervisedAlgorithm[_ <: Model.Parameters]) {

    def setSeed(value: Long): algo.type = setParam[Long, LongParam]("seed", value)

    def setNfolds(value: Int): algo.type = setParam[Int, IntParam]("nfolds", value)

    private def setParam[ValueType, ParamType <: Param[ValueType]](paramName: String, value: ValueType): algo.type = {
      val field = algo.getClass.getDeclaredFields.find(_.getName().endsWith("$$" + paramName)).head
      field.setAccessible(true)
      val parameter = field.get(algo).asInstanceOf[ParamType]
      algo.set(parameter, value)
      field.setAccessible(false)
      algo
    }
  }
}
