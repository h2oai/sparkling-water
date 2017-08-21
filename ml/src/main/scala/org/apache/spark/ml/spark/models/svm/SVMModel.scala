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

import java.lang

import hex.ModelMetricsSupervised.MetricBuilderSupervised
import hex._
import org.apache.spark.ml.spark.models.MissingValuesHandling
import water.codegen.CodeGeneratorPipeline
import water.util.{JCodeGen, SBPrintStream}
import water.{H2O, Key}

object SVMModel {

  class SVMOutput(val b: SVM) extends Model.Output(b) {
    var interceptor: Double = .0
    var iterations: Int = 0
    var weights: Array[Double] = _
    var numMeans: Array[Double] = _
  }

}

class SVMModel private[svm](val selfKey: Key[SVMModel],
                            val parms: SVMParameters,
                            val output: SVMModel.SVMOutput)
  extends Model[SVMModel, SVMParameters, SVMModel.SVMOutput](selfKey, parms, output) {

  override protected def toJavaCheckTooBig: Boolean = output.weights.length > 10000

  override def makeMetricBuilder(domain: Array[String]): MetricBuilderSupervised[Nothing] =
    _output.getModelCategory match {
      case ModelCategory.Binomial =>
        new ModelMetricsBinomial.MetricBuilderBinomial(domain)
      case ModelCategory.Regression =>
        new ModelMetricsRegression.MetricBuilderRegression
      case _ =>
        throw H2O.unimpl
    }

  private val meanImputation: Boolean = MissingValuesHandling.MeanImputation.equals(_parms._missing_values_handling)

  protected def score0(data: Array[Double], preds: Array[Double]): Array[Double] = {
    java.util.Arrays.fill(preds, 0)

    val pred =
      data.zip(_output.weights).foldRight(_output.interceptor) { case ((d, w), acc) =>
        if (meanImputation && lang.Double.isNaN(d)) {
          _output.numMeans(0) * w + acc
        } else {
          d * w + acc
        }
      }

    if (_parms._threshold.isNaN) { // Regression
      preds(0) = pred
    } else { // Binomial
      val dt = defaultThreshold()
      if (pred > _parms._threshold) {
        preds(2) = if (pred < dt) dt else pred
        preds(1) = preds(2) - 1
        preds(0) = 1
      } else {
        preds(2) = if (pred >= dt) dt - 1 else pred
        preds(1) = preds(2) + 1
        preds(0) = 0
      }
    }
    preds
  }

  override def getMojo: SVMMojoWriter = new SVMMojoWriter(this)

  override protected def toJavaInit(sb: SBPrintStream, fileCtx: CodeGeneratorPipeline): SBPrintStream = {
    val sbInitialized = super.toJavaInit(sb, fileCtx)
    sbInitialized.ip("public boolean isSupervised() { return " + isSupervised + "; }").nl
    JCodeGen.toStaticVar(sbInitialized, "WEIGHTS", _output.weights, "Weights.")
    if (meanImputation) {
      JCodeGen.toStaticVar(sbInitialized, "MEANS", _output.numMeans, "Means.")
    }
    sbInitialized
  }

  override protected def toJavaPredictBody(bodySb: SBPrintStream,
                                           classCtx: CodeGeneratorPipeline,
                                           fileCtx: CodeGeneratorPipeline,
                                           verboseCode: Boolean) {
    bodySb.i.p("java.util.Arrays.fill(preds,0);").nl
    bodySb.i.p(s"double pred = ${_output.interceptor};").nl
    bodySb.i.p("for(int i = 0; i < data.length; i++) {").nl
    if (meanImputation) {
      bodySb.i(1).p("if(Double.isNaN(data[i])) {").nl
      bodySb.i(2).p("pred += (MEANS[i] * WEIGHTS[i]);").nl
      bodySb.i(1).p("} else {").nl
    }
    bodySb.i(if (meanImputation) 2 else 1).p("pred += (data[i] * WEIGHTS[i]);").nl
    if (meanImputation) {
      bodySb.i(1).p("}").nl
    }
    bodySb.i.p("}").nl

    if (_output.nclasses == 1) {
      bodySb.i.p("preds[0] = pred;").nl
    } else {
      bodySb.i.p(s"double dt = $defaultThreshold;").nl
      bodySb.i.p(s"if(pred > ${_parms._threshold}) {").nl
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
