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
package org.apache.spark.ml.spark.models.svm

import hex.ModelMetricsSupervised.MetricBuilderSupervised
import hex._
import water.codegen.CodeGeneratorPipeline
import water.util.{JCodeGen, SBPrintStream}
import water.{H2O, Key, Keyed}

object SVMModel {

  class SVMOutput(val b: SVM) extends Model.Output(b) {
    var interceptor: Double = .0
    var iterations: Int = 0
    var weights: Array[Double] = null
  }

}

class SVMModel private[svm](val selfKey: Key[_ <: Keyed[_ <: Keyed[_ <: AnyRef]]],
                              val parms: SVMParameters,
                              val output: SVMModel.SVMOutput)
  extends Model[SVMModel, SVMParameters, SVMModel.SVMOutput](selfKey, parms, output) {

  override def makeMetricBuilder(domain: Array[String]): MetricBuilderSupervised[Nothing] =
    _output.getModelCategory match {
      case ModelCategory.Binomial =>
        new ModelMetricsBinomial.MetricBuilderBinomial(domain)
      case ModelCategory.Regression =>
        new ModelMetricsRegression.MetricBuilderRegression
      case _ =>
        throw H2O.unimpl
    }

  protected def score0(data: Array[Double], preds: Array[Double]): Array[Double] = {
    java.util.Arrays.fill(preds, 0)
    val pred =
      data.zip(_output.weights).foldRight(_output.interceptor){ case ((d, w), acc) => d * w + acc}

    if(_parms._threshold.isNaN) { // Regression
      preds(0) = pred
    } else { // Binomial
      val dt = defaultThreshold()
      if(pred > _parms._threshold) {
        preds(2) = if(pred < dt) dt else pred
        preds(1) = preds(2) - 1
        preds(0) = 1
      } else {
        preds(2) = if(pred >= dt) dt - 1 else pred
        preds(1) = preds(2) + 1
        preds(0) = 0
      }
    }
    preds
  }

  override protected def toJavaInit(sb: SBPrintStream, fileCtx: CodeGeneratorPipeline): SBPrintStream = {
    val sbInitialized = super.toJavaInit(sb, fileCtx)
    sbInitialized.ip("public boolean isSupervised() { return " + isSupervised + "; }").nl
    JCodeGen.toStaticVar(sbInitialized, "WEIGHTS", _output.weights, "Weights.")
    sbInitialized
  }

  override protected def toJavaPredictBody(bodySb: SBPrintStream,
                                           classCtx: CodeGeneratorPipeline,
                                           fileCtx: CodeGeneratorPipeline,
                                           verboseCode: Boolean) {
    bodySb.i.p("java.util.Arrays.fill(preds,0);").nl
    bodySb.i.p(s"double prediction = ${_output.interceptor};").nl
    bodySb.i.p("for(int i = 0; i < data.length; i++) {").nl
    bodySb.i(1).p("prediction += (data[i] * WEIGHTS[i]);").nl
    bodySb.i.p("}").nl

    if (_output.nclasses == 1) {
      bodySb.i.p("preds[0] = prediction;").nl
    } else {
      bodySb.i.p("double dt = defaultThreshold();")
      bodySb.i.p(s"if(prediction > ${_parms._threshold}) {").nl
      bodySb.i(1).p("preds[2] = pred < dt ? dt : pred;").nl
      bodySb.i(1).p("preds[1] = preds[2] - 1;").nl
      bodySb.i(1).p("preds[0] = 1;").nl
      bodySb.i.p(s"} else {").nl
      bodySb.i(1).p("preds[2] = pred >= dt ? dt - 1 : pred;").nl
      bodySb.i(1).p("preds[1] = preds[2] + 1;").nl
      bodySb.i(1).p("preds[0] = 0;").nl
      bodySb.i.p(s"}").nl
    }

  }

}
