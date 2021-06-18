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

import ai.h2o.sparkling.ml.models.H2OMOJOSettings
import org.apache.spark.ml.param._

trait H2OBaseMOJOParams extends Params {

  protected final val convertUnknownCategoricalLevelsToNa = new BooleanParam(
    this,
    "convertUnknownCategoricalLevelsToNa",
    "If set to 'true', the model converts unknown categorical levels to NA during making predictions.")

  protected final val convertInvalidNumbersToNa = new BooleanParam(
    this,
    "convertInvalidNumbersToNa",
    "If set to 'true', the model converts invalid numbers to NA during making predictions.")

  setDefault(
    convertUnknownCategoricalLevelsToNa -> H2OMOJOSettings.default.convertUnknownCategoricalLevelsToNa,
    convertInvalidNumbersToNa -> H2OMOJOSettings.default.convertInvalidNumbersToNa)

  def getConvertUnknownCategoricalLevelsToNa(): Boolean = $(convertUnknownCategoricalLevelsToNa)

  def getConvertInvalidNumbersToNa(): Boolean = $(convertInvalidNumbersToNa)
}
