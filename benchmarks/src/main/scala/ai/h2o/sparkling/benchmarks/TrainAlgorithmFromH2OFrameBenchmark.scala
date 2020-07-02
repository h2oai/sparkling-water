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

package ai.h2o.sparkling.benchmarks

import ai.h2o.sparkling.H2OFrame
import ai.h2o.sparkling.ml.utils.EstimatorCommonUtils

class TrainAlgorithmFromH2OFrameBenchmark(context: BenchmarkContext, algorithmBundle: AlgorithmBundle)
  extends AlgorithmBenchmarkBase[H2OFrame](context, algorithmBundle)
  with EstimatorCommonUtils {

  override protected def initialize(): H2OFrame = loadDataToH2OFrame()

  override protected def body(trainingFrame: H2OFrame): Unit = {
    val (name, params) = algorithmBundle.h2oAlgorithm
    val newParams = params ++ Map(
      "training_frame" -> trainingFrame.frameId,
      "response_column" -> context.datasetDetails.labelCol)
    trainAndGetDestinationKey(s"/3/ModelBuilders/$name", newParams)
  }
}
