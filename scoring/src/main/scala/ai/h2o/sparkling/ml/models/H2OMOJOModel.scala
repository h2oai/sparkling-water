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

import java.io.ByteArrayInputStream
import java.util

import _root_.hex.ModelCategory
import _root_.hex.genmodel.MojoReaderBackendFactory
import _root_.hex.genmodel.attributes.ModelJsonReader
import _root_.hex.genmodel.easy.EasyPredictModelWrapper
import ai.h2o.sparkling.ml.params.NullableStringParam
import ai.h2o.sparkling.ml.utils.Utils
import com.google.gson.{GsonBuilder, JsonElement}
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.collection.JavaConverters._

class H2OMOJOModel(override val uid: String) extends H2OMOJOModelBase[H2OMOJOModel] with H2OMOJOModelPredictions {

  protected final val modelDetails: NullableStringParam = new NullableStringParam(this, "modelDetails", "Raw details of this model.")

  setDefault(
    modelDetails -> null
  )

  def getModelDetails(): String = $(modelDetails)

  override protected def outputColumnName: String = getDetailedPredictionCol()

  // Some MojoModels are not serializable ( DeepLearning ), so we are reusing the mojoData to keep information about mojo model
  @transient protected lazy val easyPredictModelWrapper: EasyPredictModelWrapper = {
    val config = new EasyPredictModelWrapper.Config()
    config.setModel(Utils.getMojoModel(getMojoData()))
    config.setConvertUnknownCategoricalLevelsToNa(getConvertUnknownCategoricalLevelsToNa())
    config.setConvertInvalidNumbersToNa(getConvertInvalidNumbersToNa())
    config.setEnableContributions(getCalculateContributions())
    // always let H2O produce full output, filter later if required
    config.setUseExtendedOutput(true)
    new EasyPredictModelWrapper(config)
  }

  case class MultinomialPrediction(probabilities: Array[Double])

  case class ClusteringPrediction(cluster: Integer, distances: Array[Double])

  case class AutoEncoderPrediction(original: Array[Double], reconstructed: Array[Double])

  case class DimReductionPrediction(dimensions: Array[Double])

  case class WordEmbeddingPrediction(wordEmbeddings: util.HashMap[String, Array[Float]])

  case class AnomalyPrediction(score: Double, normalizedScore: Double)

  override protected def getPredictionSchema(): Seq[StructField] = {
    val fields = easyPredictModelWrapper.getModelCategory match {
      case ModelCategory.Binomial =>
        getBinomialPredictionSchema()
      case ModelCategory.Regression =>
        getRegressionPredictionSchema()
      case ModelCategory.Multinomial => StructField("probabilities", ArrayType(DoubleType)) :: Nil
      case ModelCategory.Clustering => StructField("cluster", DoubleType) :: Nil
      case ModelCategory.AutoEncoder => StructField("original", ArrayType(DoubleType)) :: StructField("reconstructed", ArrayType(DoubleType)) :: Nil
      case ModelCategory.DimReduction => StructField("dimensions", ArrayType(DoubleType)) :: Nil
      case ModelCategory.WordEmbedding => StructField("wordEmbeddings", DataTypes.createMapType(StringType, ArrayType(FloatType))) :: Nil
      case _ => throw new RuntimeException("Unknown model category")
    }

    Seq(StructField(getPredictionCol(), StructType(fields), nullable = false))
  }

  private def getModelUdf() = {
    val modelUdf = {
      easyPredictModelWrapper.getModelCategory match {
        case ModelCategory.Binomial =>
         getBinomialPredictionUDF()
        case ModelCategory.Regression =>
          getRegressionPredictionUDF()
        case ModelCategory.Multinomial => udf[MultinomialPrediction, Row] { r: Row =>
          val pred = easyPredictModelWrapper.predictMultinomial(RowConverter.toH2ORowData(r))
          MultinomialPrediction(pred.classProbabilities)
        }
        case ModelCategory.Clustering => udf[ClusteringPrediction, Row] { r: Row =>
          val pred = easyPredictModelWrapper.predictClustering(RowConverter.toH2ORowData(r))
          ClusteringPrediction(pred.cluster, pred.distances)
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

  override def transform(dataset: Dataset[_]): DataFrame = {
    val baseDf = applyPredictionUdf(dataset, _ => getModelUdf())

    val withPredictionDf = easyPredictModelWrapper.getModelCategory match {
      case ModelCategory.Clustering =>
        baseDf.withColumn(getPredictionCol(), col(s"${getDetailedPredictionCol()}.cluster"))
      case _ =>
        // For already existing algos, keep the functionality same,
        // We can deprecate that and slowly migrate to solution where prediction contains
        // always single value
        baseDf.withColumn(getPredictionCol(), col(getDetailedPredictionCol()))
    }

    if (getWithDetailedPredictionCol()) {
      withPredictionDf
    } else {
      withPredictionDf.drop(getDetailedPredictionCol())
    }
  }
}


trait H2OMOJOModelUtils {

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


  protected def getModelDetails(mojoData: Array[Byte]): String = {
    val is = new ByteArrayInputStream(mojoData)
    val reader = MojoReaderBackendFactory.createReaderBackend(is, MojoReaderBackendFactory.CachingStrategy.MEMORY)


    val modelOutputJson = ModelJsonReader.parseModelJson(reader).getAsJsonObject("output")
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

}

object H2OMOJOModel extends H2OMOJOReadable[H2OMOJOModel] with H2OMOJOLoader[H2OMOJOModel] with H2OMOJOModelUtils {

  override def createFromMojo(mojoData: Array[Byte], uid: String, settings: H2OMOJOSettings): H2OMOJOModel = {
    val mojoModel = Utils.getMojoModel(mojoData)

    val model = new H2OMOJOModel(uid)
    // Reconstruct state of Spark H2O MOJO transformer based on H2O's Mojo
    model.set(model.featuresCols -> mojoModel.features())
    model.set(model.convertUnknownCategoricalLevelsToNa -> settings.convertUnknownCategoricalLevelsToNa)
    model.set(model.convertInvalidNumbersToNa -> settings.convertInvalidNumbersToNa)
    model.set(model.namedMojoOutputColumns -> settings.namedMojoOutputColumns)
    model.set(model.modelDetails -> getModelDetails(mojoData))
    model.set(model.predictionCol -> settings.predictionCol)
    model.set(model.detailedPredictionCol -> settings.detailedPredictionCol)
    model.set(model.withDetailedPredictionCol -> settings.withDetailedPredictionCol)
    model.setMojoData(mojoData)
    model
  }
}

