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

import java.io.File
import ai.h2o.sparkling.{H2OContext, H2OFrame}
import ai.h2o.sparkling.backend.utils.{RestApiUtils, RestCommunication}
import ai.h2o.sparkling.ml.internals.H2OModel
import ai.h2o.sparkling.ml.params.H2OTargetEncoderProblemType
import ai.h2o.sparkling.ml.utils.SchemaUtils
import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.{MLWritable, MLWriter}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset}
import water.api.schemas3.KeyV3.FrameKeyV3

import scala.collection.JavaConverters._

class H2OTargetEncoderModel(override val uid: String, targetEncoderModel: Option[H2OModel])
  extends Model[H2OTargetEncoderModel]
  with H2OTargetEncoderBase
  with MLWritable
  with RestCommunication {

  lazy val mojoModel: H2OTargetEncoderMOJOModel = {
    val model = new H2OTargetEncoderMOJOModel()
    copyValues(model)
    targetEncoderModel match {
      case Some(targetEncoderModel) =>
        val mojo = targetEncoderModel.downloadMojo()
        model.setMojo(mojo)
      case None =>
        val emptyMojo = File.createTempFile("emptyTargetEncoder", ".mojo")
        emptyMojo.deleteOnExit()
        model.setMojo(emptyMojo)
    }
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    if (inTrainingMode) {
      transformTrainingDataset(dataset)
    } else {
      mojoModel.transform(dataset)
    }
  }

  def transformTrainingDataset(dataset: Dataset[_]): DataFrame = {
    if (getInputCols().isEmpty) {
      dataset.toDF()
    } else {
      val hc = H2OContext.ensure(
        "H2OContext needs to be created in order to use target encoding. Please create one as H2OContext.getOrCreate().")
      val temporaryColumn = getClass.getSimpleName + "_temporary_id"
      val withIdDF = dataset.withColumn(temporaryColumn, monotonically_increasing_id)
      val flatDF = SchemaUtils.flattenDataFrame(withIdDF)
      val distinctInputCols = getInputCols().flatten.distinct
      val relevantColumns = distinctInputCols ++ Array(getLabelCol(), getFoldCol(), temporaryColumn).flatMap(Option(_))
      val relevantColumnsDF = flatDF.select(relevantColumns.map(col(_)): _*)
      val input = hc.asH2OFrame(relevantColumnsDF)

      val toCategorical = if (getProblemType() == H2OTargetEncoderProblemType.Regression.name) {
        distinctInputCols
      } else {
        distinctInputCols ++ Seq(getLabelCol())
      }
      input.convertColumnsToCategorical(toCategorical)

      val conf = hc.getConf
      val endpoint = RestApiUtils.getClusterEndpoint(conf)
      val params = Map(
        "model" -> targetEncoderModel.get.modelId,
        "frame" -> input.frameId,
        "noise" -> getNoise(),
        "blending" -> getBlendedAvgEnabled(),
        "inflection_point" -> getBlendedAvgInflectionPoint(),
        "smoothing" -> getBlendedAvgSmoothing(),
        "as_training" -> true)
      val frameKeyV3 = request[FrameKeyV3](endpoint, "GET", s"/3/TargetEncoderTransform", conf, params)
      val output = H2OFrame(frameKeyV3.name)
      val inOutMapping = getInOutMapping(targetEncoderModel.get.modelId)
      val internalOutputColumns = getInputCols().map(i => inOutMapping.get(i.toSeq).get)
      val distinctInternalOutputColumns = internalOutputColumns.flatten.distinct
      val outputFrameColumns = distinctInternalOutputColumns ++ Array(temporaryColumn)
      val outputColumnsOnlyFrame = output.subframe(outputFrameColumns)
      val outputColumnsOnlyDF = hc.asSparkFrame(outputColumnsOnlyFrame.frameId)
      input.delete()
      output.delete()
      val renamedOutputColumnsOnlyDF = getOutputCols().zip(internalOutputColumns).foldLeft(outputColumnsOnlyDF) {
        case (df, (to, Seq(from))) =>
          val temporaryName = to + "_" + uid
          val dfWithTemporaryColumn = df.withColumnRenamed(from, temporaryName)
          createVectorColumn(dfWithTemporaryColumn, to, Array(temporaryName))
        case (df, (to, from)) => createVectorColumn(df, to, from.toArray)
      }
      withIdDF
        .join(renamedOutputColumnsOnlyDF, Seq(temporaryColumn), joinType = "left")
        .drop(temporaryColumn)
    }
  }

  private def getInOutMapping(modelId: String): Map[Seq[String], Seq[String]] = {
    val details = H2OModel(modelId).getDetails()
    val result = details
      .getAsJsonObject("output")
      .getAsJsonArray("input_to_output_columns")
      .iterator()
      .asScala
      .map { element =>
        val jsonObject = element.getAsJsonObject
        val from = jsonObject.getAsJsonArray("from").asScala.map(_.getAsString).toSeq
        val to = jsonObject.getAsJsonArray("to").asScala.map(_.getAsString).toSeq
        (from, to)
      }
      .toMap
    result
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
