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
/*
package ai.h2o.sparkling.ml.features

import ai.h2o.sparkling.H2OContext
import ai.h2o.sparkling.ml.internals.H2OModel
import ai.h2o.sparkling.ml.models.{H2OAutoEncoderBase, H2OAutoEncoderMOJOModel, H2OMOJOModel}
import ai.h2o.sparkling.ml.params.H2OAutoEncoderParams
import ai.h2o.sparkling.ml.utils.EstimatorCommonUtils
import org.apache.spark.sql.functions.col
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.Dataset

class H2OTempAutoEncoder(override val uid: String)
  extends Estimator[H2OAutoEncoderMOJOModel]
  with H2OAutoEncoderBase
  with H2OAutoEncoderParams
  with EstimatorCommonUtils
  with DefaultParamsWritable {

  def this() = this(Identifiable.randomUID(classOf[H2OAutoEncoder].getSimpleName))

  override def fit(dataset: Dataset[_]): H2OAutoEncoderMOJOModel = {
    val Array(training, validation) = dataset.randomSplit(Array(0.8, 0.2))
    val h2oContext = H2OContext.ensure(
      "H2OContext needs to be created in order to use auto encoding. Please create one as H2OContext.getOrCreate().")
    val input = h2oContext.asH2OFrame(training.select(getInputCols().map(col): _*))
    val valid = h2oContext.asH2OFrame(validation.select(getInputCols().map(col): _*))
    val params = getH2OAlgorithmParams(input) ++
      Map("training_frame" -> input.frameId, "model_id" -> convertModelIdToKey(getModelId()))
    val autoEncoderModelId = trainAndGetDestinationKey(s"/3/ModelBuilders/deeplearning", params)
    val mojo = H2OModel(autoEncoderModelId).downloadMojo()
    input.delete()
    valid.delete()
    val model = new H2OAutoEncoderMOJOModel(Identifiable.randomUID(classOf[H2OAutoEncoder].getSimpleName))
    model.setMojo(mojo)
    copyValues(model)
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}

object H2OTempAutoEncoder extends DefaultParamsReadable[H2OAutoEncoder]

 */
