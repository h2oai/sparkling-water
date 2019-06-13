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

import ai.h2o.automl.targetencoding.TargetEncoderModel
import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.Model
import org.apache.spark.ml.h2o.features.H2OTargetEncoderNoiseSettings
import org.apache.spark.ml.h2o.param.H2OTargetEncoderParams
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{MLWritable, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import water.support.ModelSerializationSupport

class H2OTargetEncoderModel(
    override val uid: String,
    targetEncoderModel: TargetEncoderModel)
  extends Model[H2OTargetEncoderModel] with H2OTargetEncoderParams with MLWritable {

  lazy val mojoModel: H2OTargetEncoderMojoModel = {
    val mojoData = ModelSerializationSupport.getMojoData(targetEncoderModel)
    val model = new H2OTargetEncoderMojoModel()
    copyValues(model).setMojoData(mojoData)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    if(inTrainingMode) {
      transformTrainingDataset(dataset)
    } else {
      mojoModel.transform(dataset)
    }
  }

  def transformTrainingDataset(dataset: Dataset[_]): DataFrame = {
    val h2oContext = H2OContext.getOrCreate(SparkSession.builder().getOrCreate())
    val input = h2oContext.asH2OFrame(dataset.toDF())
    changeRelevantColumnsToCategorical(input)
    val noise = Option(getNoise()).getOrElse(H2OTargetEncoderNoiseSettings(amount = 0.0))
    val holdoutStrategyId = getHoldoutStrategy().ordinal().asInstanceOf[Byte]
    val output = targetEncoderModel.transform(input, holdoutStrategyId, noise.amount, noise.seed)
    h2oContext.asDataFrame(output)
  }

  private def inTrainingMode: Boolean = {
    val stackTrace = Thread.currentThread().getStackTrace()
    stackTrace.exists(e => e.getMethodName == "fit" && e.getClassName == "org.apache.spark.ml.Pipeline")
  }

  override def copy(extra: ParamMap): H2OTargetEncoderModel = defaultCopy(extra)

  override def write: MLWriter = mojoModel.write
}
