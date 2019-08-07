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

import hex.ModelCategory
import hex.genmodel.easy.EasyPredictModelWrapper
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StructField, StructType}

trait H2OMOJOPrediction {

  def extractSimplePredictionColumns(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): Column

  def getPredictionUDF(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): UserDefinedFunction

  def getPredictionColSchema(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): Seq[StructField]

  def getDetailedPredictionColSchema(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): Seq[StructField]
}

object H2OMOJOPrediction {

  def extractPredictionColContent(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): Column = {
    predictWrapper.getModelCategory match {
      case ModelCategory.Binomial =>
        H2OMOJOPredictionBinomial.extractSimplePredictionColumns(model, predictWrapper)
      case ModelCategory.Regression =>
        H2OMOJOPredictionRegression.extractSimplePredictionColumns(model, predictWrapper)
      case ModelCategory.Multinomial =>
        H2OMOJOPredictionMultinomial.extractSimplePredictionColumns(model, predictWrapper)
      case ModelCategory.Clustering =>
        H2OMOJOPredictionClustering.extractSimplePredictionColumns(model, predictWrapper)
      case ModelCategory.AutoEncoder =>
        H2OMOJOPredictionAutoEncoder.extractSimplePredictionColumns(model, predictWrapper)
      case ModelCategory.DimReduction =>
        H2OMOJOPredictionDimReduction.extractSimplePredictionColumns(model, predictWrapper)
      case ModelCategory.WordEmbedding =>
        H2OMOJOPredictionWordEmbedding.extractSimplePredictionColumns(model, predictWrapper)
      case ModelCategory.AnomalyDetection =>
        H2OMOJOPredictionAnomaly.extractSimplePredictionColumns(model, predictWrapper)
      case _ => throw new RuntimeException("Unknown model category " + predictWrapper.getModelCategory)
    }
  }

  def getPredictionUDF(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): UserDefinedFunction = {
    predictWrapper.getModelCategory match {
      case ModelCategory.Binomial =>
        H2OMOJOPredictionBinomial.getPredictionUDF(model, predictWrapper)
      case ModelCategory.Regression =>
        H2OMOJOPredictionRegression.getPredictionUDF(model, predictWrapper)
      case ModelCategory.Multinomial =>
        H2OMOJOPredictionMultinomial.getPredictionUDF(model, predictWrapper)
      case ModelCategory.Clustering =>
        H2OMOJOPredictionClustering.getPredictionUDF(model, predictWrapper)
      case ModelCategory.AutoEncoder =>
        H2OMOJOPredictionAutoEncoder.getPredictionUDF(model, predictWrapper)
      case ModelCategory.DimReduction =>
        H2OMOJOPredictionDimReduction.getPredictionUDF(model, predictWrapper)
      case ModelCategory.WordEmbedding =>
        H2OMOJOPredictionWordEmbedding.getPredictionUDF(model, predictWrapper)
      case ModelCategory.AnomalyDetection =>
        H2OMOJOPredictionAnomaly.getPredictionUDF(model, predictWrapper)
      case _ => throw new RuntimeException("Unknown model category " + predictWrapper.getModelCategory)
    }
  }

  def getPredictionColSchema(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): Seq[StructField] = {
    val fields = predictWrapper.getModelCategory match {
      case ModelCategory.Binomial =>
        H2OMOJOPredictionBinomial.getPredictionColSchema(model, predictWrapper)
      case ModelCategory.Regression =>
        H2OMOJOPredictionRegression.getPredictionColSchema(model, predictWrapper)
      case ModelCategory.Multinomial =>
        H2OMOJOPredictionMultinomial.getPredictionColSchema(model, predictWrapper)
      case ModelCategory.Clustering =>
        H2OMOJOPredictionClustering.getPredictionColSchema(model, predictWrapper)
      case ModelCategory.AutoEncoder =>
        H2OMOJOPredictionAutoEncoder.getPredictionColSchema(model, predictWrapper)
      case ModelCategory.DimReduction =>
        H2OMOJOPredictionDimReduction.getPredictionColSchema(model, predictWrapper)
      case ModelCategory.WordEmbedding =>
        H2OMOJOPredictionWordEmbedding.getPredictionColSchema(model, predictWrapper)
      case ModelCategory.AnomalyDetection =>
        H2OMOJOPredictionAnomaly.getPredictionColSchema(model, predictWrapper)
      case _ => throw new RuntimeException("Unknown model category " + predictWrapper.getModelCategory)
    }
    Seq(StructField(model.getPredictionCol(), StructType(fields), nullable = false))
  }

  def getDetailedPredictionColSchema(@transient model: H2OMOJOModel, @transient predictWrapper: EasyPredictModelWrapper): Seq[StructField] = {
    val fields = predictWrapper.getModelCategory match {
      case ModelCategory.Binomial =>
        H2OMOJOPredictionBinomial.getDetailedPredictionColSchema(model, predictWrapper)
      case ModelCategory.Regression =>
        H2OMOJOPredictionRegression.getDetailedPredictionColSchema(model, predictWrapper)
      case ModelCategory.Multinomial =>
        H2OMOJOPredictionMultinomial.getDetailedPredictionColSchema(model, predictWrapper)
      case ModelCategory.Clustering =>
        H2OMOJOPredictionClustering.getDetailedPredictionColSchema(model, predictWrapper)
      case ModelCategory.AutoEncoder =>
        H2OMOJOPredictionAutoEncoder.getDetailedPredictionColSchema(model, predictWrapper)
      case ModelCategory.DimReduction =>
        H2OMOJOPredictionDimReduction.getDetailedPredictionColSchema(model, predictWrapper)
      case ModelCategory.WordEmbedding =>
        H2OMOJOPredictionWordEmbedding.getDetailedPredictionColSchema(model, predictWrapper)
      case ModelCategory.AnomalyDetection =>
        H2OMOJOPredictionAnomaly.getDetailedPredictionColSchema(model, predictWrapper)
      case _ => throw new RuntimeException("Unknown model category " + predictWrapper.getModelCategory)
    }
    Seq(StructField(model.getDetailedPredictionCol(), StructType(fields), nullable = false))
  }
}


