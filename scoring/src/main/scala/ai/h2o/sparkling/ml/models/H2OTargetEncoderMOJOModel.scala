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

import _root_.hex.genmodel.algos.targetencoder.TargetEncoderMojoModel
import _root_.hex.genmodel.easy.EasyPredictModelWrapper
import ai.h2o.sparkling.ml.utils.Utils
import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Dataset, Row}

class H2OTargetEncoderMOJOModel(override val uid: String)
  extends Model[H2OTargetEncoderMOJOModel]
  with H2OTargetEncoderBase
  with H2OMOJOWritable
  with H2OMOJOFlattenedInput {

  override protected def inputColumnNames: Array[String] = getInputCols()

  override protected def outputColumnName: String = getClass.getSimpleName + "_output"

  def this() = this(Identifiable.randomUID(getClass.getSimpleName))

  @transient private lazy val orderOfInputColumns = {
    val mojoModel = Utils.getMojoModel(getMojo()).asInstanceOf[TargetEncoderMojoModel]
    val indexes = mojoModel._columnNameToIdx
    indexes
  }

  override def transform(dataset: Dataset[_]): DataFrame = {
    import org.apache.spark.sql.DatasetExtensions._
    val outputCols = getOutputCols()
    val udfWrapper = H2OTargetEncoderMOJOUdfWrapper(getMojo(), outputCols)
    val withPredictionsDF = applyPredictionUdf(dataset, _ => udfWrapper.mojoUdf)
    withPredictionsDF
      .withColumns(
        outputCols,
        outputCols.zip(getInputCols()).map {
          case (outputCol, inputCol) =>
            val index = orderOfInputColumns.get(inputCol)
            col(outputColumnName)(index) as outputCol
        })
      .drop(outputColumnName)
  }

  override def copy(extra: ParamMap): H2OTargetEncoderMOJOModel = defaultCopy(extra)
}

/**
  * The class holds all necessary dependencies of udf that needs to be serialized.
  */
case class H2OTargetEncoderMOJOUdfWrapper(mojo: File, outputCols: Array[String]) {
  @transient private lazy val easyPredictModelWrapper: EasyPredictModelWrapper = {
    val model = Utils.getMojoModel(mojo).asInstanceOf[TargetEncoderMojoModel]
    val config = new EasyPredictModelWrapper.Config()
    config.setModel(model)
    config.setConvertUnknownCategoricalLevelsToNa(true)
    config.setConvertInvalidNumbersToNa(true)
    new EasyPredictModelWrapper(config)
  }

  val mojoUdf = udf[Array[Option[Double]], Row] { r: Row =>
    val inputRowData = RowConverter.toH2ORowData(r)
    try {
      val prediction = easyPredictModelWrapper.predictTargetEncoding(inputRowData)
      prediction.transformations.map(Some(_))
    } catch {
      case _: Throwable => outputCols.map(_ => None)
    }
  }
}

object H2OTargetEncoderMOJOModel extends H2OMOJOReadable[H2OTargetEncoderMOJOModel]
