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

import ai.h2o.sparkling.H2OFrame
import org.apache.spark.expose.Logging
trait HasTreatmentCol extends ParameterConstructorMethods with Logging {
  protected val treatmentCol = stringParam(
    name = "treatmentCol",
    doc =
      """Define the column which will be used for computing uplift gain to select best split for a tree. The column has to divide the dataset into treatment (value 1) and control (value 0) groups.""")

  def getTreatmentCol(): String = $(treatmentCol)

  def setTreatmentCol(value: String): this.type = {
    set(treatmentCol, value)
  }

  private[sparkling] def getTreatmentColParam(trainingFrame: H2OFrame): Map[String, Any] = {
    Map("treatment_column" -> getTreatmentCol())
  }

  setDefault(treatmentCol -> "treatment")

}
