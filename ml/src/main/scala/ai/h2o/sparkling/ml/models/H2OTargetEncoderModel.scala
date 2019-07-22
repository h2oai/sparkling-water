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

import ai.h2o.automl.targetencoding.TargetEncoderModel
import ai.h2o.sparkling.ml.features.H2OTargetEncoderBase
import org.apache.spark.h2o.H2OContext
import org.apache.spark.h2o.utils.H2OSchemaUtils
import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{MLWritable, MLWriter}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}
import org.apache.spark.sql.functions._
import water.support.ModelSerializationSupport

class H2OTargetEncoderModel(
    override val uid: String,
    targetEncoderModel: TargetEncoderModel)
  extends Model[H2OTargetEncoderModel] with H2OTargetEncoderBase with MLWritable {

  lazy val mojoModel: H2OTargetEncoderMojoModel = {
    val mojoData = ModelSerializationSupport.getMojoData(targetEncoderModel)
    val model = new H2OTargetEncoderMojoModel()
    copyValues(model).setMojoData(mojoData)
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    if (inTrainingMode) {
      transformTrainingDataset(dataset)
    } else {
      mojoModel.transform(dataset)
    }
  }

  def transformTrainingDataset(dataset: Dataset[_]): DataFrame = {
    val h2oContext = H2OContext.getOrCreate(SparkSession.builder().getOrCreate())
    val temporaryColumn = getClass.getSimpleName + "_temporary_id"
    val withIdDF = dataset.withColumn(temporaryColumn, monotonically_increasing_id)
    val flatDF = H2OSchemaUtils.flattenDataFrame(withIdDF)
    val relevantColumns = getInputCols() ++ Array(getLabelCol(), getFoldCol(), temporaryColumn).flatMap(Option(_))
    val relevantColumnsDF = flatDF.select(relevantColumns.map(col(_)): _*)
    val input = h2oContext.asH2OFrame(relevantColumnsDF)
    convertRelevantColumnsToCategorical(input)
    val holdoutStrategyId = possibleHoldoutStrategyValues.indexOf(getHoldoutStrategy().toLowerCase).asInstanceOf[Byte]
    val outputFrame = try {
      targetEncoderModel.transform(input, holdoutStrategyId, getNoise(), getNoiseSeed())
    } catch {
      case e: IllegalStateException if e.getMessage.contains("We do not support multi-class target case") =>
        throw new RuntimeException(
          "An unexpected value. The label column can contain only values that were present in the training dataset")
    }
    val outputColumnsOnlyFrame = outputFrame.subframe(getOutputCols() ++ Array(temporaryColumn))
    val outputColumnsOnlyDF = h2oContext.asDataFrame(outputColumnsOnlyFrame)
    withIdDF
      .join(outputColumnsOnlyDF, Seq(temporaryColumn), joinType="left")
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
