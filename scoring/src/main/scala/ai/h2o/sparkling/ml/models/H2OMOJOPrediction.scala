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
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{StructField, StructType}

trait H2OMOJOPrediction
  extends H2OMOJOPredictionRegression
  with H2OMOJOPredictionWordEmbedding
  with H2OMOJOPredictionAnomaly
  with H2OMOJOPredictionAutoEncoder
  with H2OMOJOPredictionMultinomial
  with H2OMOJOPredictionDimReduction
  with H2OMOJOPredictionClustering
  with H2OMOJOPredictionBinomial
  with H2OMOJOPredictionOrdinal {
  self: H2OMOJOModel =>

  def extractPredictionColContent(): Column = {
    val predictWrapper = H2OMOJOCache.getMojoBackend(uid, getMojo, this)
    predictWrapper.getModelCategory match {
      case ModelCategory.Binomial => extractBinomialPredictionColContent()
      case ModelCategory.Regression => extractRegressionPredictionColContent()
      case ModelCategory.Multinomial => extractMultinomialPredictionColContent()
      case ModelCategory.Clustering => extractClusteringPredictionColContent()
      case ModelCategory.AutoEncoder => extractAutoEncoderPredictionColContent()
      case ModelCategory.DimReduction => extractDimReductionSimplePredictionColContent()
      case ModelCategory.WordEmbedding => extractWordEmbeddingPredictionColContent()
      case ModelCategory.AnomalyDetection => extractAnomalyPredictionColContent()
      case ModelCategory.Ordinal => extractOrdinalPredictionColContent()
      case _ => throw new RuntimeException("Unknown model category " + predictWrapper.getModelCategory)
    }
  }

  def getPredictionUDF(): UserDefinedFunction = {
    SQLConf.get.setConfString("spark.sql.legacy.allowUntypedScalaUDF", "true")
    val predictWrapper = H2OMOJOCache.getMojoBackend(uid, getMojo, this)
    predictWrapper.getModelCategory match {
      case ModelCategory.Binomial => getBinomialPredictionUDF()
      case ModelCategory.Regression => getRegressionPredictionUDF()
      case ModelCategory.Multinomial => getMultinomialPredictionUDF()
      case ModelCategory.Clustering => getClusteringPredictionUDF()
      case ModelCategory.AutoEncoder => getAutoEncoderPredictionUDF()
      case ModelCategory.DimReduction => getDimReductionPredictionUDF()
      case ModelCategory.WordEmbedding => getWordEmbeddingPredictionUDF()
      case ModelCategory.AnomalyDetection => getAnomalyPredictionUDF()
      case ModelCategory.Ordinal => getOrdinalPredictionUDF()
      case _ => throw new RuntimeException("Unknown model category " + predictWrapper.getModelCategory)
    }
  }

  override def getPredictionColSchema(): Seq[StructField] = {
    val predictWrapper = H2OMOJOCache.getMojoBackend(uid, getMojo, this)
    predictWrapper.getModelCategory match {
      case ModelCategory.Binomial => getBinomialPredictionColSchema()
      case ModelCategory.Regression => getRegressionPredictionColSchema()
      case ModelCategory.Multinomial => getMultinomialPredictionColSchema()
      case ModelCategory.Clustering => getClusteringPredictionColSchema()
      case ModelCategory.AutoEncoder => getAutoEncoderPredictionColSchema()
      case ModelCategory.DimReduction => getDimReductionPredictionColSchema()
      case ModelCategory.WordEmbedding => getWordEmbeddingPredictionColSchema()
      case ModelCategory.AnomalyDetection => getAnomalyPredictionColSchema()
      case ModelCategory.Ordinal => getOrdinalPredictionColSchema()
      case _ => throw new RuntimeException("Unknown model category " + predictWrapper.getModelCategory)
    }
  }

  override def getDetailedPredictionColSchema(): Seq[StructField] = {
    Seq(StructField(getDetailedPredictionCol(), getPredictionSchema(), nullable = true))
  }

  def getPredictionSchema(): StructType = {
    val predictWrapper = H2OMOJOCache.getMojoBackend(uid, getMojo, this)
    predictWrapper.getModelCategory match {
      case ModelCategory.Binomial => getBinomialPredictionSchema()
      case ModelCategory.Regression => getRegressionPredictionSchema()
      case ModelCategory.Multinomial => getMultinomialPredictionSchema()
      case ModelCategory.Clustering => getClusteringPredictionSchema()
      case ModelCategory.AutoEncoder => getAutoEncoderPredictionSchema()
      case ModelCategory.DimReduction => getDimReductionPredictionSchema()
      case ModelCategory.WordEmbedding => getWordEmbeddingPredictionSchema()
      case ModelCategory.AnomalyDetection => getAnomalyPredictionSchema()
      case ModelCategory.Ordinal => getOrdinalPredictionSchema()
      case _ => throw new RuntimeException("Unknown model category " + predictWrapper.getModelCategory)
    }
  }
}
