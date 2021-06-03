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

import ai.h2o.sparkling.ml.params.H2OAutoEncoderMOJOParams
import ai.h2o.sparkling.ml.utils.Utils
import hex.genmodel.easy.EasyPredictModelWrapper
import org.apache.spark.ml.Model
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{udf, col}

class H2OAutoEncoderMOJOModel(override val uid: String)
  extends Model[H2OAutoEncoderMOJOModel]
  with H2OAutoEncoderBase
  with H2OAutoEncoderMOJOParams
  with H2OMOJOWritable
  with H2OMOJOFlattenedInput {

  override protected def inputColumnNames: Array[String] = getInputCols()

  override protected def outputColumnName: String = getClass.getSimpleName + "_output"

  def this() = this(Identifiable.randomUID(getClass.getSimpleName))

  override def transform(dataset: Dataset[_]): DataFrame = {
    val udfWrapper = H2OAutoEncoderMOJOUdfWrapper(getMojo)
    val withPredictionsDF = applyPredictionUdf(dataset, _ => udfWrapper.mojoUdf)
    withPredictionsDF.select(
      col("*"),
      col(s"$outputColumnName._1") as "original",
      col(s"$outputColumnName._2") as "reconstructed",
      col(s"$outputColumnName._3") as "mse")
      .drop(outputColumnName)
  }

  override def copy(extra: ParamMap): H2OAutoEncoderMOJOModel = defaultCopy(extra)
}


object H2OAutoEncoderMOJOModel extends H2OSpecificMOJOLoader[H2OAutoEncoderMOJOModel]

case class H2OAutoEncoderMOJOUdfWrapper(mojoGetter: () => File) {

  @transient private lazy val mojoModel = Utils.getMojoModel(mojoGetter())
  @transient private lazy val easyPredictModelWrapper: EasyPredictModelWrapper = {
    val config = new EasyPredictModelWrapper.Config()
    config.setModel(mojoModel)
    config.setConvertUnknownCategoricalLevelsToNa(true)
    config.setConvertInvalidNumbersToNa(true)
    new EasyPredictModelWrapper(config)
  }

  val mojoUdf: UserDefinedFunction =
    udf[(Option[Array[Double]], Option[Array[Double]], Option[Double]), Row] { r: Row =>
      val inputRowData = RowConverter.toH2ORowData(r)
      try {
        val prediction = easyPredictModelWrapper.predictAutoEncoder(inputRowData)
        (Some(prediction.original), Some(prediction.reconstructed), Some(prediction.mse))
      } catch {
        case _: Throwable => (None, None, None)
      }
    }
}
