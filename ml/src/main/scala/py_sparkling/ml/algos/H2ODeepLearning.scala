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
package py_sparkling.ml.algos

import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.deeplearning.{DeepLearning, DeepLearningModel}
import org.apache.spark.ml.util.DefaultParamsReadable

/**
  * H2O DeepLearning Wrapper for PySparkling
  */
class H2ODeepLearning(override val uid: String) extends org.apache.spark.ml.h2o.algos.H2ODeepLearning(uid) {

  override def trainModel(params: DeepLearningParameters): DeepLearningModel = new DeepLearning(params).trainModel().get()
}

private[algos] object H2ODeepLearning extends DefaultParamsReadable[H2ODeepLearning]
