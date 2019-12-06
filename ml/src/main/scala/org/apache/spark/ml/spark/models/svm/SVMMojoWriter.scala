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

package org.apache.spark.ml.spark.models.svm

import hex.ModelMojoWriter
import org.apache.spark.ml.spark.models.MissingValuesHandling

class SVMMojoWriter(svmModel: SparkSVMModel) extends ModelMojoWriter[SparkSVMModel, SparkSVMParameters, SparkSVMModel.SparkSVMOutput](svmModel) {

  def this() {
    this(null)
  }

  override def writeModelData(): Unit = {
    if (MissingValuesHandling.MeanImputation == model._parms._missing_values_handling) {
      writekv("meanImputation", true)
      writekv("means", model.output.numMeans)
    }

    writekv("weights", model.output.weights)
    writekv("interceptor", model.output.interceptor)
    writekv("defaultThreshold", model._parms._threshold)
    writekv("threshold", model._parms._threshold)
  }

  override def mojoVersion(): String = "1.00"
}
