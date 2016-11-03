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
package org.apache.spark.ml.spark.models.als

import com.github.fommil.netlib.BLAS.{getInstance => blas}
import hex.ModelMetricsSupervised.MetricBuilderSupervised
import hex.{Model, ModelCategory, ModelMetricsRegression}
import org.apache.spark.rdd.RDD
import water.codegen.CodeGeneratorPipeline
import water.util.SBPrintStream
import water.{H2O, Key}

object ALSModel {

  class ALSOutput(val b: ALS) extends Model.Output(b) {
    var rank: Int = 0
    var userFeatures: RDD[(Int, Array[Double])] = _
    var productFeatures: RDD[(Int, Array[Double])] = _
  }

}

class ALSModel private[als](val selfKey: Key[ALSModel],
                            val parms: ALSParameters,
                            val output: ALSModel.ALSOutput)
  extends Model[ALSModel, ALSParameters, ALSModel.ALSOutput](selfKey, parms, output) {

  override def makeMetricBuilder(domain: Array[String]): MetricBuilderSupervised[Nothing] =
    _output.getModelCategory match {
      case ModelCategory.Regression =>
        new ModelMetricsRegression.MetricBuilderRegression
      case _ =>
        throw H2O.unimpl
    }

  protected def score0(data: Array[Double], preds: Array[Double]): Array[Double] = {
    val userVector = _output.userFeatures.lookup(data(0).toInt).head
    val productVector = _output.productFeatures.lookup(data(1).toInt).head
    preds(0) = blas.ddot(_output.rank, userVector, 1, productVector, 1)
    preds
  }

  override protected def toJavaPredictBody(bodySb: SBPrintStream,
                                           classCtx: CodeGeneratorPipeline,
                                           fileCtx: CodeGeneratorPipeline,
                                           verboseCode: Boolean) = ???
}
