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

import java.io._
import java.util

import _root_.hex.genmodel.easy.prediction._
import hex.ModelCategory
import hex.genmodel.easy.{EasyPredictModelWrapper, RowData}
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.h2o.utils.H2OSchemaUtils
import org.apache.spark.ml.h2o.param.H2OModelParams
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Model => SparkModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, _}
import org.apache.spark.{ml, mllib}
import water.support.ModelSerializationSupport

import scala.reflect.{ClassTag, classTag}
class H2OMOJOModel(val mojoData: Array[Byte], override val uid: String)
  extends SparkModel[H2OMOJOModel] with H2OModelParams with MLWritable {

  def this(mojoData: Array[Byte]) = this(mojoData, Identifiable.randomUID("mojoModel"))

  // Some MojoModels are not serializable ( DeepLearning ), so we are reusing the mojoData to keep information about mojo model
  @transient var easyPredictModelWrapper: EasyPredictModelWrapper = _

  case class BinomialPrediction(p0: Double, p1: Double)

  case class RegressionPrediction(value: Double)

  case class MultinomialPrediction(probabilities: Array[Double])

  case class ClusteringPrediction(cluster: Integer)

  case class AutoEncoderPrediction(original: Array[Double], reconstructed: Array[Double])

  case class DimReductionPrediction(dimensions: Array[Double])

  case class WordEmbeddingPrediction(wordEmbeddings: util.HashMap[String, Array[Float]])

  def predictionSchema(): Seq[StructField] = {
    val fields = getOrCreateEasyModelWrapper().getModelCategory match {
      case ModelCategory.Binomial => StructField("p0", DoubleType) :: StructField("p1", DoubleType) :: Nil
      case ModelCategory.Regression => StructField("value", DoubleType) :: Nil
      case ModelCategory.Multinomial => StructField("probabilities", ArrayType(DoubleType)) :: Nil
      case ModelCategory.Clustering => StructField("cluster", DoubleType) :: Nil
      case ModelCategory.AutoEncoder => StructField("original", ArrayType(DoubleType)) :: StructField("reconstructed", ArrayType(DoubleType)) :: Nil
      case ModelCategory.DimReduction => StructField("dimensions", ArrayType(DoubleType)) :: Nil
      case ModelCategory.WordEmbedding => StructField("wordEmbeddings", DataTypes.createMapType(StringType, ArrayType(FloatType))) :: Nil
      case _ => throw new RuntimeException("Unknown model category")
    }

    Seq(StructField($(outputCol), StructType(fields), nullable = false))
  }


  implicit def toBinomialPrediction(pred: AbstractPrediction) = BinomialPrediction(
    pred.asInstanceOf[BinomialModelPrediction].classProbabilities(0),
    pred.asInstanceOf[BinomialModelPrediction].classProbabilities(1))

  implicit def toRegressionPrediction(pred: AbstractPrediction) = RegressionPrediction(
    pred.asInstanceOf[RegressionModelPrediction].value)

  implicit def toMultinomialPrediction(pred: AbstractPrediction) = MultinomialPrediction(
    pred.asInstanceOf[MultinomialModelPrediction].classProbabilities)

  implicit def toClusteringPrediction(pred: AbstractPrediction) = ClusteringPrediction(
    pred.asInstanceOf[ClusteringModelPrediction].cluster)

  implicit def toAutoEncoderPrediction(pred: AbstractPrediction) = AutoEncoderPrediction(
    pred.asInstanceOf[AutoEncoderModelPrediction].original,
    pred.asInstanceOf[AutoEncoderModelPrediction].reconstructed)

  implicit def toDimReductionPrediction(pred: AbstractPrediction) = DimReductionPrediction(
    pred.asInstanceOf[DimReductionModelPrediction].dimensions)

  implicit def toWordEmbeddingPrediction(pred: AbstractPrediction) = WordEmbeddingPrediction(
    pred.asInstanceOf[Word2VecPrediction].wordEmbeddings)

  def getModelUdf() = {
    val modelUdf = {
      getOrCreateEasyModelWrapper().getModelCategory match {
        case ModelCategory.Binomial => udf[BinomialPrediction, Row] { r: Row =>
          getOrCreateEasyModelWrapper().predict(rowToRowData(r))
        }
        case ModelCategory.Regression => udf[RegressionPrediction, Row] { r: Row =>
          getOrCreateEasyModelWrapper().predict(rowToRowData(r))
        }
        case ModelCategory.Multinomial => udf[MultinomialPrediction, Row] { r: Row =>
          getOrCreateEasyModelWrapper().predict(rowToRowData(r))
        }
        case ModelCategory.Clustering => udf[ClusteringPrediction, Row] { r: Row =>
          getOrCreateEasyModelWrapper().predict(rowToRowData(r))
        }
        case ModelCategory.AutoEncoder => udf[AutoEncoderPrediction, Row] { r: Row =>
          getOrCreateEasyModelWrapper().predict(rowToRowData(r))
        }
        case ModelCategory.DimReduction => udf[DimReductionPrediction, Row] { r: Row =>
          getOrCreateEasyModelWrapper().predict(rowToRowData(r))
        }
        case ModelCategory.WordEmbedding => udf[WordEmbeddingPrediction, Row] { r: Row =>
          getOrCreateEasyModelWrapper().predict(rowToRowData(r))
        }
        case _ => throw new RuntimeException("Unknown model category")
      }
    }
    modelUdf
  }


  override def copy(extra: ParamMap): H2OMOJOModel = defaultCopy(extra)

  def defaultFileName: String = H2OMOJOModel.defaultFileName

  private def getOrCreateEasyModelWrapper() = {
    if (easyPredictModelWrapper == null) {
      val config = new EasyPredictModelWrapper.Config()
      config.setModel(ModelSerializationSupport.getMojoModel(mojoData))
      config.setConvertUnknownCategoricalLevelsToNa($(convertUnknownCategoricalLevelsToNa))
      easyPredictModelWrapper = new EasyPredictModelWrapper(config)
    }
    easyPredictModelWrapper
  }


  override def transform(dataset: Dataset[_]): DataFrame = {
    val flatten = H2OSchemaUtils.flattenDataFrame(dataset.toDF())
    val args = flatten.schema.fields.map(f => flatten(f.name))
    flatten.select(col("*"), getModelUdf()(struct(args: _*)).as($(outputCol)))
  }


  private def rowToRowData(row: Row): RowData = new RowData {
    row.schema.fields.zipWithIndex.foreach { case (f, idxRow) =>
      if (row.get(idxRow) != null) {
        f.dataType match {
          case BooleanType =>
            if (row.getBoolean(idxRow)) put(f.name, 1.toString) else put(f.name, 0.toString)
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

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    // Here we should check validity of input schema however
    // in theory user can pass invalid schema with missing columns
    // and model will be able to still provide a prediction
    StructType(schema.fields ++ predictionSchema)
  }

  @Since("1.6.0")
  override def write: MLWriter = new H2OMOJOModelWriter(this)

}

private[models] class H2OMOJOModelWriter(instance: H2OMOJOModel) extends MLWriter {

  @org.apache.spark.annotation.Since("1.6.0")
  override protected def saveImpl(path: String): Unit = {
    DefaultParamsWriter.saveMetadata(instance, path, sc)
    val outputPath = new Path(path, instance.defaultFileName)
    val fs = outputPath.getFileSystem(sc.hadoopConfiguration)
    val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val out = fs.create(qualifiedOutputPath)
    try {
      out.write(instance.mojoData)
    } finally {
      out.close()
    }
    logInfo(s"Saved to: $qualifiedOutputPath")
  }
}

private[models] class H2OMOJOModelReader[T <: H2OMOJOModel : ClassTag]
(val defaultFileName: String) extends MLReader[T] {

  @org.apache.spark.annotation.Since("1.6.0")
  override def load(path: String): T = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc)

    val inputPath =  new Path(path, defaultFileName)
    val fs = inputPath.getFileSystem(sc.hadoopConfiguration)
    val qualifiedInputPath = inputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val is = fs.open(qualifiedInputPath)

    val mojoData = Stream.continually(is.read()).takeWhile(_ != -1).map(_.toByte).toArray

    val h2oModel = make(mojoData, metadata.uid)(sqlContext)
    DefaultParamsReader.getAndSetParams(h2oModel, metadata)
    h2oModel
  }

  def make(mojoData: Array[Byte], uid: String)(sqLContext: SQLContext): T = {
    classTag[T].runtimeClass.getConstructor(classOf[Array[Byte]], classOf[String]).
      newInstance(mojoData, uid).asInstanceOf[T]
  }
}

class H2OMOJOModelHelper[T<: H2OMOJOModel](implicit m: ClassTag[T]) extends MLReadable[T]{
  val defaultFileName = "mojo_model"

  @Since("1.6.0")
  override def read: MLReader[T] = new H2OMOJOModelReader[T](defaultFileName)

  @Since("1.6.0")
  override def load(path: String): T = super.load(path)

  def createFromMojo(path: String): T = {
    val inputPath =  new Path(path)
    val fs = inputPath.getFileSystem(SparkSession.builder().getOrCreate().sparkContext.hadoopConfiguration)
    val qualifiedInputPath = inputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val is = fs.open(qualifiedInputPath)

    createFromMojo(is, new File(path).getName)
  }

  def createFromMojo(is: InputStream, uid: String = Identifiable.randomUID("mojoModel")): T = {
    val mojoData = Stream.continually(is.read).takeWhile(_ != -1).map(_.toByte).toArray
    val mojoModel = ModelSerializationSupport.getMojoModel(mojoData)
    val sparkMojoModel = m.runtimeClass.getConstructor(classOf[Array[Byte]], classOf[String]).
      newInstance(mojoData, uid).asInstanceOf[T]
    // Reconstruct state of Spark H2O MOJO transformer based on H2O's Mojo
    sparkMojoModel.setFeaturesCols(mojoModel.getNames.filter(_ != mojoModel.getResponseName))
    sparkMojoModel.setPredictionsCol(mojoModel.getResponseName)
    sparkMojoModel
  }
}

object H2OMOJOModel extends H2OMOJOModelHelper[H2OMOJOModel] {}
