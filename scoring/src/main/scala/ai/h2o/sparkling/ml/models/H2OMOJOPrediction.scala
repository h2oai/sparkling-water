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
import org.apache.spark.sql.Column
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.{StructField, StructType}

trait H2OMOJOPrediction
  extends H2OMOJOPredictionRegression
  with H2OMOJOPredictionWordEmbedding
  with H2OMOJOPredictionAnomaly
  with H2OMOJOPredictionMultinomial
  with H2OMOJOPredictionDimReduction
  with H2OMOJOPredictionClustering
  with H2OMOJOPredictionBinomial
  with H2OMOJOPredictionCoxPH
  with H2OMOJOPredictionOrdinal {
  self: H2OAlgorithmMOJOModel =>

  def extractPredictionColContent(): Column = {
    val predictWrapper = H2OMOJOCache.getMojoBackend(uid, getMojo)
    predictWrapper.getModelCategory match {
      case ModelCategory.Binomial => extractBinomialPredictionColContent()
      case ModelCategory.Regression => extractRegressionPredictionColContent()
      case ModelCategory.Multinomial => extractMultinomialPredictionColContent()
      case ModelCategory.Clustering => extractClusteringPredictionColContent()
      case ModelCategory.DimReduction => extractDimReductionSimplePredictionColContent()
      case ModelCategory.WordEmbedding => extractWordEmbeddingPredictionColContent()
      case ModelCategory.AnomalyDetection => extractAnomalyPredictionColContent()
      case ModelCategory.Ordinal => extractOrdinalPredictionColContent()
      case ModelCategory.CoxPH => extractCoxPHPredictionColContent()
      case _ => throw new RuntimeException("Unknown model category " + predictWrapper.getModelCategory)
    }
  }

  def getPredictionUDF(): UserDefinedFunction = {
    val mojoModel = H2OMOJOCache.getMojoBackend(uid, getMojo)
    val schema = getPredictionSchema()
    val configInitializers = getEasyPredictModelWrapperConfigurationInitializers()
    mojoModel.getModelCategory match {
      case ModelCategory.Binomial => getBinomialPredictionUDF(schema, uid, mojoFileName, configInitializers)
      case ModelCategory.Regression => getRegressionPredictionUDF(schema, uid, mojoFileName, configInitializers)
      case ModelCategory.Multinomial => getMultinomialPredictionUDF(schema, uid, mojoFileName, configInitializers)
      case ModelCategory.Clustering => getClusteringPredictionUDF(schema, uid, mojoFileName, configInitializers)
      case ModelCategory.DimReduction => getDimReductionPredictionUDF(schema, uid, mojoFileName, configInitializers)
      case ModelCategory.WordEmbedding =>
        getWordEmbeddingPredictionUDF(schema, uid, mojoFileName, configInitializers, getFeaturesCols())
      case ModelCategory.AnomalyDetection => getAnomalyPredictionUDF(schema, uid, mojoFileName, configInitializers)
      case ModelCategory.Ordinal => getOrdinalPredictionUDF(schema, uid, mojoFileName, configInitializers)
      case ModelCategory.CoxPH => getCoxPHPredictionUDF(schema, uid, mojoFileName, configInitializers)
      case _ => throw new RuntimeException("Unknown model category " + mojoModel.getModelCategory)
    }
  }

  def getPredictionColSchema(): Seq[StructField] = {
    val predictWrapper = H2OMOJOCache.getMojoBackend(uid, getMojo)
    predictWrapper.getModelCategory match {
      case ModelCategory.Binomial => getBinomialPredictionColSchema()
      case ModelCategory.Regression => getRegressionPredictionColSchema()
      case ModelCategory.Multinomial => getMultinomialPredictionColSchema()
      case ModelCategory.Clustering => getClusteringPredictionColSchema()
      case ModelCategory.DimReduction => getDimReductionPredictionColSchema()
      case ModelCategory.WordEmbedding => getWordEmbeddingPredictionColSchema()
      case ModelCategory.AnomalyDetection => getAnomalyPredictionColSchema()
      case ModelCategory.Ordinal => getOrdinalPredictionColSchema()
      case ModelCategory.CoxPH => getCoxPHPredictionColSchema()
      case _ => throw new RuntimeException("Unknown model category " + predictWrapper.getModelCategory)
    }
  }

  def getDetailedPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getDetailedPredictionCol(), getPredictionSchema(), nullable = true))
  }

  def getPredictionSchema(): StructType = {
    val predictWrapper = H2OMOJOCache.getMojoBackend(uid, getMojo)
    predictWrapper.getModelCategory match {
      case ModelCategory.Binomial => getBinomialPredictionSchema()
      case ModelCategory.Regression => getRegressionPredictionSchema()
      case ModelCategory.Multinomial => getMultinomialPredictionSchema()
      case ModelCategory.Clustering => getClusteringPredictionSchema()
      case ModelCategory.DimReduction => getDimReductionPredictionSchema()
      case ModelCategory.WordEmbedding => getWordEmbeddingPredictionSchema()
      case ModelCategory.AnomalyDetection => getAnomalyPredictionSchema()
      case ModelCategory.Ordinal => getOrdinalPredictionSchema()
      case ModelCategory.CoxPH => getCoxPHPredictionSchema()
      case _ => throw new RuntimeException("Unknown model category " + predictWrapper.getModelCategory)
    }
  }
}
