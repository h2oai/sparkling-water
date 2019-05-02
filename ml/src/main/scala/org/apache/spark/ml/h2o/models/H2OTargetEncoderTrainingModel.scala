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
package org.apache.spark.ml.h2o.models

import ai.h2o.automl.targetencoding.TargetEncoder
import org.apache.spark.h2o.H2OContext
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import water.fvec.Frame

class H2OTargetEncoderTrainingModel(
    override val uid: String,
    targetEncoder: TargetEncoder,
    encodingMap: java.util.Map[String, Frame],
    trainingDataset: Dataset[_])
  extends H2OTargetEncoderModel(uid, H2OTargetEncoderTrainingModel.convertEncodingMap(encodingMap)) {

  override def transform(dataset: Dataset[_]): DataFrame = {
    if (dataset.eq(trainingDataset)) {
      val h2oContext = H2OContext.getOrCreate(SparkSession.builder().getOrCreate())
      val input = h2oContext.asH2OFrame(dataset.toDF())
      val output = targetEncoder.applyTargetEncoding(input, getLabelCol(), encodingMap, 2, false, false, 0L)
      h2oContext.asDataFrame(output)
    } else {
      super.transform(dataset)
    }
  }
}

object H2OTargetEncoderTrainingModel {
  def convertEncodingMap(encodingMap: java.util.Map[String, Frame]): Map[String, Map[String, Array[Int]]] = {
    ??? // TODO: This needs to be implemented H20-3 and exposed out.
  }
}
