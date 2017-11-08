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

import hex.ModelCategory
import hex.genmodel.easy.{EasyPredictModelWrapper, RowData}
import org.apache.spark._
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.ml.h2o.param.H2OModelParams
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Model => SparkModel}
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import water.support.ModelSerializationSupport

import scala.reflect.ClassTag

class H2OMOJOModel(val mojoData: Array[Byte], override val uid: String)
  extends SparkModel[H2OMOJOModel] with H2OModelParams with MLWritable {

  def this(mojoData: Array[Byte]) = this(mojoData, Identifiable.randomUID("mojoModel"))

  // Some MojoModels are not serializable ( DeepLearning ), so we are reusing the mojoData to keep information about mojo model
  @transient var easyPredictModelWrapper: EasyPredictModelWrapper = createEasyPredictModelWrapper()

  override def copy(extra: ParamMap): H2OMOJOModel = defaultCopy(extra)

  private def flattenSchemaToCol(schema: StructType, prefix: String = null): Array[Column] = {
    import org.apache.spark.sql.functions.col

    schema.fields.flatMap { f =>
      val colName = if (prefix == null) f.name else prefix + "." + f.name

      f.dataType match {
        case st: StructType => flattenSchemaToCol(st, colName)
        case _ => Array[Column](col(colName))
      }
    }
  }

  def defaultFileName: String = H2OMOJOModel.defaultFileName

  private def flattenDataFrame(df: DataFrame): DataFrame = {
    import org.apache.spark.sql.functions.col
    val flattenSchema = flattenSchemaToCol(df.schema)
    // this is needed so the flattened data frame has hierarchical names
    val renamedCols = flattenSchema.map(name => col(name.toString()).as(name.toString()))
    df.select(renamedCols: _*)
  }

  private def createEasyPredictModelWrapper() = new EasyPredictModelWrapper(ModelSerializationSupport.getMojoModel(mojoData))

  override def transform(dataset: Dataset[_]): DataFrame = {

    val spark = SparkSession.builder().getOrCreate()
    val df = flattenDataFrame(dataset.toDF())

    val predictedRows = df.rdd.map { row =>
      // create predict wrapper from mojo data on each executor where predictions take place
      easyPredictModelWrapper = createEasyPredictModelWrapper()
      // create RowData on which we do the predictions
      val dt = new RowData
      val values = row.schema.fields.zipWithIndex.map { case (entry, idxRow) =>

        if ($(featuresCols).contains(entry.name)) {
          setRowData(row, idxRow, dt, entry) // use only relevant columns for training
        }

        if (row.isNullAt(idxRow)) {
          0
        } else {
          row.get(idxRow)
        }
      }
      Row.merge(Row.fromSeq(values.toSeq), predict(dt))
    }
    spark.createDataFrame(predictedRows, StructType(df.schema.fields ++ getPredictionFrameSchema()))
  }


  def setRowData(row: Row, idxRow: Int, dt: RowData, entry: StructField) {
    if (row.isNullAt(idxRow)) {
      dt.put(entry.name, 0.toString) // 0 as NA
    } else {
      entry.dataType match {
        case BooleanType =>
          if (row.getBoolean(idxRow)) dt.put(entry.name, 1.toString) else dt.put(entry.name, 0.toString)
        case BinaryType =>
          row.getAs[Array[Byte]](idxRow).zipWithIndex.foreach { case (v, idx) =>
            dt.put(entry.name + idx, v.toString)
          }
        case ByteType => dt.put(entry.name, row.getByte(idxRow).toString)
        case ShortType => dt.put(entry.name, row.getShort(idxRow).toString)
        case IntegerType => dt.put(entry.name, row.getInt(idxRow).toString)
        case LongType => dt.put(entry.name, row.getLong(idxRow).toString)
        case FloatType => dt.put(entry.name, row.getFloat(idxRow).toString)
        case _: DecimalType => dt.put(entry.name, row.getDecimal(idxRow).doubleValue().toString)
        case DoubleType => dt.put(entry.name, row.getDouble(idxRow).toString)
        case StringType => dt.put(entry.name, row.getString(idxRow))
        case TimestampType => dt.put(entry.name, row.getAs[java.sql.Timestamp](idxRow).getTime.toString)
        case DateType => dt.put(entry.name, row.getAs[java.sql.Date](idxRow).getTime.toString)
        case ArrayType(_, _) => // for now assume that all arrays and vecs have the same size - we can store max size as part of the model
          row.getAs[Seq[_]](idxRow).zipWithIndex.foreach { case (v, idx) =>
            dt.put(entry.name + idx, v.toString)
          }
        case _: UserDefinedType[_ /*mllib.linalg.Vector*/ ] =>
          val value = row.get(idxRow)
          value match {
            case vector: mllib.linalg.Vector =>
              (0 until vector.size).foreach { idx => // WRONG this patter needs to share the same code as in the data transformation
                dt.put(entry.name + idx, vector(idx).toString)
              }
            case vector: ml.linalg.Vector =>
              (0 until vector.size).foreach { idx =>
                dt.put(entry.name + idx, vector(idx).toString)
              }
          }
        case _ => dt.put(entry.name, dt.get(idxRow).toString)
      }
    }
  }

  def getPredictionFrameSchema(): Seq[StructField] = {
    easyPredictModelWrapper.getModelCategory match {
      case ModelCategory.Binomial => easyPredictModelWrapper.getResponseDomainValues.map(StructField(_, DoubleType))
      case ModelCategory.Multinomial => easyPredictModelWrapper.getResponseDomainValues.map(StructField(_, DoubleType))
      case ModelCategory.Regression => Seq(StructField("value", DoubleType))
      case ModelCategory.Clustering => Seq(StructField("cluster", DoubleType))
      case ModelCategory.AutoEncoder => throw new RuntimeException("Unimplemented model category")
      case ModelCategory.DimReduction => throw new RuntimeException("Unimplemented model category")
      case ModelCategory.WordEmbedding => throw new RuntimeException("Unimplemented model category")
      case _ => throw new RuntimeException("Unknown model category")
    }
  }

  def predict(data: RowData): Row = {
    easyPredictModelWrapper.getModelCategory
    match {
      case ModelCategory.Binomial => Row(easyPredictModelWrapper.predictBinomial(data).classProbabilities: _*)
      case ModelCategory.Multinomial => Row(easyPredictModelWrapper.predictMultinomial(data).classProbabilities: _*)
      case ModelCategory.Regression => Row(easyPredictModelWrapper.predictRegression(data).value)
      case ModelCategory.Clustering => Row(easyPredictModelWrapper.predictClustering(data).cluster)
      case ModelCategory.AutoEncoder => throw new RuntimeException("Unimplemented model category")
      case ModelCategory.DimReduction => throw new RuntimeException("Unimplemented model category")
      case ModelCategory.WordEmbedding => throw new RuntimeException("Unimplemented model category")
      case _ => throw new RuntimeException("Unknown model category")
    }
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    // Here we should check validity of input schema however
    // in theory user can pass invalid schema with missing columns
    // and model will be able to still provide a prediction
    StructType(schema.fields ++ getPredictionFrameSchema)
  }

  @Since("1.6.0")
  override def write: MLWriter = new H2OMOJOModelWriter(this)

}

private[models] class H2OMOJOModelWriter(instance: H2OMOJOModel) extends MLWriter {

  @org.apache.spark.annotation.Since("1.6.0")
  override protected def saveImpl(path: String): Unit = {
    DefaultParamsWriter.saveMetadata(instance, path, sc)
    val file = new java.io.File(path, instance.defaultFileName)
    val fos = new FileOutputStream(file)
    try {
      fos.write(instance.mojoData)
    } finally {
      fos.close()
    }
  }
}

private[models] class H2OMOJOModelReader
(val defaultFileName: String) extends MLReader[H2OMOJOModel] {

  private val className = implicitly[ClassTag[H2OMOJOModel]].runtimeClass.getName

  @org.apache.spark.annotation.Since("1.6.0")
  override def load(path: String): H2OMOJOModel = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
    val file = new File(path, defaultFileName)
    val is = new FileInputStream(file)
    val mojoData = Stream.continually(is.read).takeWhile(_ != -1).map(_.toByte).toArray

    val h2oModel = make(mojoData, metadata.uid)(sqlContext)
    DefaultParamsReader.getAndSetParams(h2oModel, metadata)
    h2oModel
  }

  def make(mojoData: Array[Byte], uid: String)(sqLContext: SQLContext): H2OMOJOModel = {
    new H2OMOJOModel(mojoData, uid)
  }
}

object H2OMOJOModel extends MLReadable[H2OMOJOModel] {
  val defaultFileName = "mojo_model"

  @Since("1.6.0")
  override def read: MLReader[H2OMOJOModel] = new H2OMOJOModelReader(defaultFileName)

  @Since("1.6.0")
  override def load(path: String): H2OMOJOModel = super.load(path)

  def createFromMojo(is: InputStream, uid: String = Identifiable.randomUID("mojoModel")): H2OMOJOModel = {

    val mojoData = Stream.continually(is.read).takeWhile(_ != -1).map(_.toByte).toArray
    val mojoModel = ModelSerializationSupport.getMojoModel(mojoData)
    val sparkMojoModel = new H2OMOJOModel(mojoData, uid)

    // Reconstruct state of Spark H2O MOJO transformer based on H2O's Mojo
    sparkMojoModel.setFeaturesCols(mojoModel.getNames.filter(_ != mojoModel.getResponseName))
    sparkMojoModel.setPredictionsCol(mojoModel.getResponseName)
    sparkMojoModel
  }
}
