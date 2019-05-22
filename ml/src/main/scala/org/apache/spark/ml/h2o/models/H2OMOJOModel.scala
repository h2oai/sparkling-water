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

import java.util

import hex.ModelCategory
import hex.genmodel.easy.{EasyPredictModelWrapper, RowData}
import org.apache.spark.h2o.utils.H2OSchemaUtils
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.{ml, mllib}
import py_sparkling.ml.models.{H2OMOJOModel => PyH2OMOJOModel}
import water.support.ModelSerializationSupport

class H2OMOJOModel(override val uid: String) extends H2OMOJOModelBase[H2OMOJOModel] {

  // Some MojoModels are not serializable ( DeepLearning ), so we are reusing the mojoData to keep information about mojo model
  @transient private var easyPredictModelWrapper: EasyPredictModelWrapper = _

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
    val fields = getOrCreateEasyModelWrapper().getModelCategory match {
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
    getOrCreateEasyModelWrapper().m.calibrateClassProbabilities(Array.fill[Double](2)(0))
  }

  private def getModelUdf() = {
    val modelUdf = {
      getOrCreateEasyModelWrapper().getModelCategory match {
        case ModelCategory.Binomial =>
          if (supportsCalibratedProbabilities()) {
            udf[BinomialPredictionExtended, Row] { r: Row =>
              val pred = getOrCreateEasyModelWrapper().predictBinomial(rowToRowData(r))
              BinomialPredictionExtended(
                pred.classProbabilities(0),
                pred.classProbabilities(1),
                pred.calibratedClassProbabilities(0),
                pred.calibratedClassProbabilities(1)
              )
            }
          } else {
            udf[BinomialPrediction, Row] { r: Row =>
              val pred = getOrCreateEasyModelWrapper().predictBinomial(rowToRowData(r))
              BinomialPrediction(
                pred.classProbabilities(0),
                pred.classProbabilities(1)
              )
            }
          }

        case ModelCategory.Regression => udf[RegressionPrediction, Row] { r: Row =>
          val pred = getOrCreateEasyModelWrapper().predictRegression(rowToRowData(r))
          RegressionPrediction(pred.value)
        }
        case ModelCategory.Multinomial => udf[MultinomialPrediction, Row] { r: Row =>
          val pred = getOrCreateEasyModelWrapper().predictMultinomial(rowToRowData(r))
          MultinomialPrediction(pred.classProbabilities)
        }
        case ModelCategory.Clustering => udf[ClusteringPrediction, Row] { r: Row =>
          val pred = getOrCreateEasyModelWrapper().predictClustering(rowToRowData(r))
          ClusteringPrediction(pred.cluster)
        }
        case ModelCategory.AutoEncoder => udf[AutoEncoderPrediction, Row] { r: Row =>
          val pred = getOrCreateEasyModelWrapper().predictAutoEncoder(rowToRowData(r))
          AutoEncoderPrediction(pred.original, pred.reconstructed)
        }
        case ModelCategory.DimReduction => udf[DimReductionPrediction, Row] { r: Row =>
          val pred = getOrCreateEasyModelWrapper().predictDimReduction(rowToRowData(r))
          DimReductionPrediction(pred.dimensions)
        }
        case ModelCategory.WordEmbedding => udf[WordEmbeddingPrediction, Row] { r: Row =>
          val pred = getOrCreateEasyModelWrapper().predictWord2Vec(rowToRowData(r))
          WordEmbeddingPrediction(pred.wordEmbeddings)
        }
        case ModelCategory.AnomalyDetection => udf[AnomalyPrediction, Row] { r: Row =>
          val pred = getOrCreateEasyModelWrapper().predictAnomalyDetection(rowToRowData(r))
          AnomalyPrediction(pred.score, pred.normalizedScore)
        }
        case _ => throw new RuntimeException("Unknown model category " + getOrCreateEasyModelWrapper().getModelCategory)
      }
    }
    modelUdf
  }

  override def copy(extra: ParamMap): H2OMOJOModel = defaultCopy(extra)

  private def getOrCreateEasyModelWrapper() = {
    if (easyPredictModelWrapper == null) {
      val config = new EasyPredictModelWrapper.Config()
      config.setModel(ModelSerializationSupport.getMojoModel(getMojoData))
      config.setConvertUnknownCategoricalLevelsToNa(getConvertUnknownCategoricalLevelsToNa())
      easyPredictModelWrapper = new EasyPredictModelWrapper(config)
    }
    easyPredictModelWrapper
  }


  override def transform(dataset: Dataset[_]): DataFrame = {
    val flattenedDF = H2OSchemaUtils.flattenDataFrame(dataset.toDF())
    val relevantColumnNames = flattenedDF.columns.intersect(getFeaturesCols())
    val args = relevantColumnNames.map(flattenedDF(_))
    flattenedDF.select(col("*"), getModelUdf()(struct(args: _*)).as(getPredictionCol()))
  }


  private def rowToRowData(row: Row): RowData = new RowData {
    row.schema.fields.zipWithIndex.foreach { case (f, idxRow) =>
      if (row.get(idxRow) != null) {
        f.dataType match {
          case BooleanType =>
            put(f.name, row.getBoolean(idxRow).toString)
          case BinaryType =>
            row.getAs[Array[Byte]](idxRow).zipWithIndex.foreach { case (v, idx) =>
              put(f.name + idx, v.toString)
            }
          case ByteType => put(f.name, row.getByte(idxRow).toString)
          case ShortType => put(f.name, row.getShort(idxRow).toString)
          case IntegerType => put(f.name, row.getInt(idxRow).toString)
          case LongType => put(f.name, row.getLong(idxRow).toString)
          case FloatType => put(f.name, row.getFloat(idxRow).toString)
          case _: DecimalType => put(f.name, row.getDecimal(idxRow).doubleValue().toString)
          case DoubleType => put(f.name, row.getDouble(idxRow).toString)
          case StringType => put(f.name, row.getString(idxRow))
          case TimestampType => put(f.name, row.getAs[java.sql.Timestamp](idxRow).getTime.toString)
          case DateType => put(f.name, row.getAs[java.sql.Date](idxRow).getTime.toString)
          case ArrayType(_, _) => // for now assume that all arrays and vecs have the same size - we can store max size as part of the model
            row.getAs[Seq[_]](idxRow).zipWithIndex.foreach { case (v, idx) =>
              put(f.name + idx, v.toString)
            }
          case _: UserDefinedType[_ /*mllib.linalg.Vector*/ ] =>
            val value = row.get(idxRow)
            value match {
              case vector: mllib.linalg.Vector =>
                (0 until vector.size).foreach { idx => // WRONG this patter needs to share the same code as in the data transformation
                  put(f.name + idx, vector(idx).toString)
                }
              case vector: ml.linalg.Vector =>
                (0 until vector.size).foreach { idx =>
                  put(f.name + idx, vector(idx).toString)
                }
            }
          case null => // no op
          case _ => put(f.name, get(idxRow).toString)
        }
      }
    }
  }
}


object H2OMOJOModel extends H2OMOJOReadable[PyH2OMOJOModel] with H2OMOJOLoader[PyH2OMOJOModel] {

  override def createFromMojo(mojoData: Array[Byte], uid: String): PyH2OMOJOModel = {
    val mojoModel = ModelSerializationSupport.getMojoModel(mojoData)
    val model = new PyH2OMOJOModel(uid)
    // Reconstruct state of Spark H2O MOJO transformer based on H2O's Mojo
    model.set(model.featuresCols -> mojoModel.features())
    model.setMojoData(mojoData)
    model
  }
}
