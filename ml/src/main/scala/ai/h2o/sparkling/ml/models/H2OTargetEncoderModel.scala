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

import ai.h2o.sparkling.backend.utils.{RestApiUtils, RestCommunication}
import ai.h2o.sparkling.frame.H2OFrame
import ai.h2o.sparkling.ml.features.H2OTargetEncoderModelUtils
import ai.h2o.sparkling.ml.utils.SchemaUtils
import ai.h2o.sparkling.model.H2OModel
import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{MLWritable, MLWriter}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import water.api.schemas3.KeyV3.FrameKeyV3

class H2OTargetEncoderModel(override val uid: String, targetEncoderModel: H2OModel)
  extends Model[H2OTargetEncoderModel]
    with H2OTargetEncoderBase
    with MLWritable
    with H2OTargetEncoderModelUtils
    with RestCommunication {

  lazy val mojoModel: H2OTargetEncoderMOJOModel = {
    val mojoData = targetEncoderModel.downloadMojoData()
    val model = new H2OTargetEncoderMOJOModel()
    copyValues(model).setMojoData(mojoData)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    if (inTrainingMode) {
      transformTrainingDataset(dataset)
    } else {
      mojoModel.transform(dataset)
    }
  }

  private def inputColumnNameToInternalOutputName(inputColumnName: String): String = inputColumnName + "_te"

  def transformTrainingDataset(dataset: Dataset[_]): DataFrame = {
    val hc = H2OContext.ensure("H2OContext needs to be created in order to use target encoding. Please create one as H2OContext.getOrCreate().")
    val temporaryColumn = getClass.getSimpleName + "_temporary_id"
    val withIdDF = dataset.withColumn(temporaryColumn, monotonically_increasing_id)
    val flatDF = SchemaUtils.flattenDataFrame(withIdDF)
    val relevantColumns = getInputCols() ++ Array(getLabelCol(), getFoldCol(), temporaryColumn).flatMap(Option(_))
    val relevantColumnsDF = flatDF.select(relevantColumns.map(col(_)): _*)
    val input = hc.asH2OFrameKeyString(relevantColumnsDF)
    convertRelevantColumnsToCategorical(input)
    val internalOutputColumns = getInputCols().map(inputColumnNameToInternalOutputName)
    val outputFrameColumns = internalOutputColumns ++ Array(temporaryColumn)
    val conf = hc.getConf
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val params = Map(
      "model" -> targetEncoderModel.modelId,
      "frame" -> input,
      "data_leakage_handling" -> getHoldoutStrategy(),
      "noise" -> getNoise(),
      "blending" -> getBlendedAvgEnabled(),
      "inflection_point" -> getBlendedAvgInflectionPoint(),
      "smoothing" -> getBlendedAvgSmoothing(),
      "seed" -> getNoiseSeed()
    )
    val frameKeyV3 = request[FrameKeyV3](endpoint, "GET", s"/3/TargetEncoderTransform", conf, params)
    val outputColumnsOnlyFrame = H2OFrame(frameKeyV3.name).subframe(outputFrameColumns)
    val outputColumnsOnlyDF = hc.asDataFrame(outputColumnsOnlyFrame.frameId)
    val renamedOutputColumnsOnlyDF = getOutputCols().zip(internalOutputColumns).foldLeft(outputColumnsOnlyDF) {
      case (df, (to, from)) => df.withColumnRenamed(from, to)
    }
    withIdDF
      .join(renamedOutputColumnsOnlyDF, Seq(temporaryColumn), joinType = "left")
      .drop(temporaryColumn)
  }

  private def inTrainingMode: Boolean = {
    val stackTrace = Thread.currentThread().getStackTrace()
    stackTrace.exists(e => e.getMethodName == "fit" && e.getClassName == "org.apache.spark.ml.Pipeline")
  }

  override def copy(extra: ParamMap): H2OTargetEncoderModel = {
    defaultCopy[H2OTargetEncoderModel](extra).setParent(parent)
  }

  override def write: MLWriter = mojoModel.write
}
