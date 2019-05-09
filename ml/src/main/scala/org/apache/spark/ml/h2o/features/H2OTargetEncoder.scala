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
package org.apache.spark.ml.h2o.features

import ai.h2o.automl.targetencoding.TargetEncoder
import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.h2o.models.{H2OTargetEncoderModel, H2OTargetEncoderTrainingModel}
import org.apache.spark.ml.h2o.param.H2OTargetEncoderParams
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.{Dataset, SparkSession}

class H2OTargetEncoder(override val uid: String)
  extends Estimator[H2OTargetEncoderModel]
  with H2OTargetEncoderParams
  with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID("H2OTargetEncoder"))

  override def fit(dataset: Dataset[_]): H2OTargetEncoderModel = {
    val h2oContext = H2OContext.getOrCreate(SparkSession.builder().getOrCreate())
    val input = h2oContext.asH2OFrame(dataset.toDF())
    val targetEncoder = new TargetEncoder(getInputCols())
    val encodingMap = targetEncoder.prepareEncodingMap(input, getLabelCol(), getFoldCol())
    val model = new H2OTargetEncoderTrainingModel(uid, targetEncoder, encodingMap, dataset).setParent(this)
    copyValues(model)
  }

  override def copy(extra: ParamMap): H2OTargetEncoder = defaultCopy(extra)
}

object H2OTargetEncoder extends DefaultParamsReadable[H2OTargetEncoder]
