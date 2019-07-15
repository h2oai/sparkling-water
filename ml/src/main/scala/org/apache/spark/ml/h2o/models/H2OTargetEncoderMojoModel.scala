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

import hex.genmodel.algos.targetencoder.TargetEncoderMojoModel
import hex.genmodel.easy.EasyPredictModelWrapper
import org.apache.spark.h2o.converters.RowConverter
import org.apache.spark.ml.Model
import org.apache.spark.ml.h2o.features.H2OTargetEncoderBase
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.util.Identifiable
import water.support.ModelSerializationSupport

class H2OTargetEncoderMojoModel(override val uid: String) extends Model[H2OTargetEncoderMojoModel]
  with H2OTargetEncoderBase with H2OMOJOWritable with H2OMOJOFlattenedInput {

  override protected def inputColumnNames: Array[String] = getInputCols()

  override protected def outputColumnName: String = this.getClass.getSimpleName + "_output"

  def this() = this(Identifiable.randomUID("H2OTargetEncoderMojoModel"))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val outputCols = getOutputCols()
    val udfWrapper = H2OTargetEncoderMojoUdfWrapper(getMojoData(), outputCols)
    val withPredictionsDF = applyPredictionUdf(dataset, _ => udfWrapper.mojoUdf)
    withPredictionsDF
      .withColumns(outputCols, outputCols.zipWithIndex.map{ case (c, i) => col(outputColumnName)(i) as c })
      .drop(outputColumnName)
  }

  override def copy(extra: ParamMap): H2OTargetEncoderMojoModel = defaultCopy(extra)
}

/**
  * The class holds all necessary dependencies of udf that needs to be serialized.
  */
case class H2OTargetEncoderMojoUdfWrapper(mojoData: Array[Byte], outputCols: Array[String]) {
  @transient private lazy val easyPredictModelWrapper: EasyPredictModelWrapper = {
    val model = ModelSerializationSupport
      .getMojoModel(mojoData)
      .asInstanceOf[TargetEncoderMojoModel]
    val config = new EasyPredictModelWrapper.Config()
    config.setModel(model)
    config.setConvertUnknownCategoricalLevelsToNa(true)
    config.setConvertInvalidNumbersToNa(true)
    new EasyPredictModelWrapper(config)
  }

  val mojoUdf = udf[Array[Option[Double]], Row] { r: Row =>
    val inputRowData = RowConverter.toH2ORowData(r)
    try {
      val prediction = easyPredictModelWrapper.transformWithTargetEncoding(inputRowData)
      prediction.transformations.map(Some(_))
    } catch {
      case _: Throwable => outputCols.map(_ => None)
    }
  }
}

object H2OTargetEncoderMojoModel extends H2OMOJOReadable[H2OTargetEncoderMojoModel]