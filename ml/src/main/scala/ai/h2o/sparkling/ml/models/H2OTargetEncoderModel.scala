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

import ai.h2o.sparkling.ml.features.H2OTargetEncoderModelUtils
import ai.h2o.sparkling.ml.utils.SchemaUtils
import ai.h2o.sparkling.utils.SparkSessionUtils
import ai.h2o.targetencoding.{BlendingParams, TargetEncoder, TargetEncoderModel}
import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{MLWritable, MLWriter}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import water.support.ModelSerializationSupport

class H2OTargetEncoderModel(
    override val uid: String,
    targetEncoderModel: TargetEncoderModel)
  extends Model[H2OTargetEncoderModel] with H2OTargetEncoderBase with MLWritable
  with H2OTargetEncoderModelUtils {

  lazy val mojoModel: H2OTargetEncoderMOJOModel = {
    val mojoData = ModelSerializationSupport.getMojoData(targetEncoderModel)
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
    val h2oContext = H2OContext.getOrCreate(SparkSessionUtils.active)
    val temporaryColumn = getClass.getSimpleName + "_temporary_id"
    val withIdDF = dataset.withColumn(temporaryColumn, monotonically_increasing_id)
    val flatDF = SchemaUtils.flattenDataFrame(withIdDF)
    val relevantColumns = getInputCols() ++ Array(getLabelCol(), getFoldCol(), temporaryColumn).flatMap(Option(_))
    val relevantColumnsDF = flatDF.select(relevantColumns.map(col(_)): _*)
    val input = h2oContext.asH2OFrame(relevantColumnsDF)
    convertRelevantColumnsToCategorical(input)
    val holdoutStrategyId = TargetEncoder.DataLeakageHandlingStrategy.valueOf(getHoldoutStrategy()).ordinal().asInstanceOf[Byte]
    val blendingParams = new BlendingParams(getBlendedAvgInflectionPoint(), getBlendedAvgSmoothing())
    val outputFrame = targetEncoderModel.transform(
      input,
      holdoutStrategyId,
      getNoise(),
      getBlendedAvgEnabled(),
      blendingParams,
      getNoiseSeed())
    val internalOutputColumns = getInputCols().map(inputColumnNameToInternalOutputName)
    val outputColumnsOnlyFrame = outputFrame.subframe(internalOutputColumns ++ Array(temporaryColumn))
    val outputColumnsOnlyDF = h2oContext.asDataFrame(outputColumnsOnlyFrame)
    val renamedOutputColumnsOnlyDF = getOutputCols().zip(internalOutputColumns).foldLeft(outputColumnsOnlyDF) {
      case (df, (to, from)) => df.withColumnRenamed(from, to)
    }
    withIdDF
      .join(renamedOutputColumnsOnlyDF, Seq(temporaryColumn), joinType="left")
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
