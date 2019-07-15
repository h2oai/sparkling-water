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

import java.io.ByteArrayInputStream
import java.util

import _root_.hex.genmodel.MojoReaderBackendFactory
import _root_.hex.genmodel.descriptor.JsonModelDescriptorReader
import com.google.gson.{GsonBuilder, JsonElement}
import hex.ModelCategory
import hex.genmodel.easy.EasyPredictModelWrapper
import org.apache.spark.h2o.converters.RowConverter
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import py_sparkling.ml.models.{H2OMOJOModel => PyH2OMOJOModel}
import water.support.ModelSerializationSupport

import scala.collection.JavaConverters._

class H2OMOJOModel(override val uid: String) extends H2OMOJOModelBase[H2OMOJOModel] {

  // Some MojoModels are not serializable ( DeepLearning ), so we are reusing the mojoData to keep information about mojo model
  @transient private lazy val easyPredictModelWrapper: EasyPredictModelWrapper = {
    val config = new EasyPredictModelWrapper.Config()
    config.setModel(ModelSerializationSupport.getMojoModel(getMojoData()))
    config.setConvertUnknownCategoricalLevelsToNa(getConvertUnknownCategoricalLevelsToNa())
    config.setConvertInvalidNumbersToNa(getConvertInvalidNumbersToNa())
    new EasyPredictModelWrapper(config)
  }

  case class BinomialPrediction(p0: Double, p1: Double)

  case class BinomialPredictionExtended(p0: Double, p1: Double, p0_calibrated: Double, p1_calibrated: Double)

  case class RegressionPrediction(value: Double)

  case class MultinomialPrediction(probabilities: Array[Double])

  case class ClusteringPrediction(cluster: Integer)

  case class AutoEncoderPrediction(original: Array[Double], reconstructed: Array[Double])

  case class DimReductionPrediction(dimensions: Array[Double])

  case class WordEmbeddingPrediction(wordEmbeddings: util.HashMap[String, Array[Float]])

  case class AnomalyPrediction(score: Double, normalizedScore: Double)

  override protected def getPredictionSchema(): Seq[StructField] = {
    val fields = easyPredictModelWrapper.getModelCategory match {
      case ModelCategory.Binomial =>
        val binomialSchemaBase = Seq("p0", "p1")
        val binomialSchema = if (supportsCalibratedProbabilities()) {
          binomialSchemaBase ++ Seq("p0_calibrated", "p1_calibrated")
        } else {
          binomialSchemaBase
        }
        binomialSchema.map(StructField(_, DoubleType, nullable = false))
      case ModelCategory.Regression => StructField("value", DoubleType) :: Nil
      case ModelCategory.Multinomial => StructField("probabilities", ArrayType(DoubleType)) :: Nil
      case ModelCategory.Clustering => StructField("cluster", DoubleType) :: Nil
      case ModelCategory.AutoEncoder => StructField("original", ArrayType(DoubleType)) :: StructField("reconstructed", ArrayType(DoubleType)) :: Nil
      case ModelCategory.DimReduction => StructField("dimensions", ArrayType(DoubleType)) :: Nil
      case ModelCategory.WordEmbedding => StructField("wordEmbeddings", DataTypes.createMapType(StringType, ArrayType(FloatType))) :: Nil
      case _ => throw new RuntimeException("Unknown model category")
    }

    Seq(StructField(getPredictionCol(), StructType(fields), nullable = false))
  }

  private def supportsCalibratedProbabilities(): Boolean = {
    // calibrateClassProbabilities returns false if model does not support calibrated probabilities,
    // however it also accepts array of probabilities to calibrate. We are not interested in calibration,
    // but to call this method, we need to pass dummy array of size 2 with default values to 0.
    easyPredictModelWrapper.m.calibrateClassProbabilities(Array.fill[Double](2)(0))
  }

  private def getModelUdf() = {
    val modelUdf = {
      easyPredictModelWrapper.getModelCategory match {
        case ModelCategory.Binomial =>
          if (supportsCalibratedProbabilities()) {
            udf[BinomialPredictionExtended, Row] { r: Row =>
              val pred = easyPredictModelWrapper.predictBinomial(RowConverter.toH2ORowData(r))
              BinomialPredictionExtended(
                pred.classProbabilities(0),
                pred.classProbabilities(1),
                pred.calibratedClassProbabilities(0),
                pred.calibratedClassProbabilities(1)
              )
            }
          } else {
            udf[BinomialPrediction, Row] { r: Row =>
              val pred = easyPredictModelWrapper.predictBinomial(RowConverter.toH2ORowData(r))
              BinomialPrediction(
                pred.classProbabilities(0),
                pred.classProbabilities(1)
              )
            }
          }

        case ModelCategory.Regression => udf[RegressionPrediction, Row] { r: Row =>
          val pred = easyPredictModelWrapper.predictRegression(RowConverter.toH2ORowData(r))
          RegressionPrediction(pred.value)
        }
        case ModelCategory.Multinomial => udf[MultinomialPrediction, Row] { r: Row =>
          val pred = easyPredictModelWrapper.predictMultinomial(RowConverter.toH2ORowData(r))
          MultinomialPrediction(pred.classProbabilities)
        }
        case ModelCategory.Clustering => udf[ClusteringPrediction, Row] { r: Row =>
          val pred = easyPredictModelWrapper.predictClustering(RowConverter.toH2ORowData(r))
          ClusteringPrediction(pred.cluster)
        }
        case ModelCategory.AutoEncoder => udf[AutoEncoderPrediction, Row] { r: Row =>
          val pred = easyPredictModelWrapper.predictAutoEncoder(RowConverter.toH2ORowData(r))
          AutoEncoderPrediction(pred.original, pred.reconstructed)
        }
        case ModelCategory.DimReduction => udf[DimReductionPrediction, Row] { r: Row =>
          val pred = easyPredictModelWrapper.predictDimReduction(RowConverter.toH2ORowData(r))
          DimReductionPrediction(pred.dimensions)
        }
        case ModelCategory.WordEmbedding => udf[WordEmbeddingPrediction, Row] { r: Row =>
          val pred = easyPredictModelWrapper.predictWord2Vec(RowConverter.toH2ORowData(r))
          WordEmbeddingPrediction(pred.wordEmbeddings)
        }
        case ModelCategory.AnomalyDetection => udf[AnomalyPrediction, Row] { r: Row =>
          val pred = easyPredictModelWrapper.predictAnomalyDetection(RowConverter.toH2ORowData(r))
          AnomalyPrediction(pred.score, pred.normalizedScore)
        }
        case _ => throw new RuntimeException("Unknown model category " + easyPredictModelWrapper.getModelCategory)
      }
    }
    modelUdf
  }

  override def copy(extra: ParamMap): H2OMOJOModel = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = applyPredictionUdf(dataset, _ => getModelUdf())
}


object H2OMOJOModel extends H2OMOJOReadable[PyH2OMOJOModel] with H2OMOJOLoader[PyH2OMOJOModel] {

  private def removeMetaField(json: JsonElement): JsonElement = {
    if (json.isJsonObject) {
      json.getAsJsonObject.remove("__meta")
      json.getAsJsonObject.entrySet().asScala.foreach(entry => removeMetaField(entry.getValue))
    }
    if (json.isJsonArray) {
      json.getAsJsonArray.asScala.foreach(removeMetaField)
    }
    json
  }


  private def getModelDetails(mojoData: Array[Byte]): String = {
    val is = new ByteArrayInputStream(mojoData)
    val reader = MojoReaderBackendFactory.createReaderBackend(is, MojoReaderBackendFactory.CachingStrategy.MEMORY)

    val modelOutputJson = JsonModelDescriptorReader.parseModelJson(reader).getAsJsonObject("output")
    if (modelOutputJson == null) {
      "Model details not available!"
    } else {
      removeMetaField(modelOutputJson)
      modelOutputJson.remove("domains")
      modelOutputJson.remove("help")
      val gson = new GsonBuilder().setPrettyPrinting().create
      val prettyJson = gson.toJson(modelOutputJson)
      prettyJson
    }
  }

  override def createFromMojo(mojoData: Array[Byte], uid: String, settings: H2OMOJOSettings): PyH2OMOJOModel = {
    val mojoModel = ModelSerializationSupport.getMojoModel(mojoData)

    val model = new PyH2OMOJOModel(uid)
    // Reconstruct state of Spark H2O MOJO transformer based on H2O's Mojo
    model.set(model.featuresCols -> mojoModel.features())
    model.set(model.convertUnknownCategoricalLevelsToNa -> settings.convertUnknownCategoricalLevelsToNa)
    model.set(model.convertInvalidNumbersToNa -> settings.convertInvalidNumbersToNa)
    model.set(model.namedMojoOutputColumns -> settings.namedMojoOutputColumns)
    model.set(model.modelDetails -> getModelDetails(mojoData))
    model.setMojoData(mojoData)
    model
  }
}
