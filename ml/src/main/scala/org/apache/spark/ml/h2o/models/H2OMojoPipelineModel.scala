package org.apache.spark.ml.h2o.models

import java.io._

import ai.h2o.mojos.runtime.MojoPipeline
import ai.h2o.mojos.runtime.readers.MojoPipelineReaderBackendFactory
import org.apache.spark.annotation.Since
import org.apache.spark.h2o.utils.H2OSchemaUtils
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Model => SparkModel}
import org.apache.spark.sql.functions.{col, struct, udf}
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SQLContext}

import scala.reflect.ClassTag


class H2OMojoPipelineModel(val mojoData: Array[Byte], override val uid: String)
  extends SparkModel[H2OMojoPipelineModel] with MLWritable {

  @transient private var model: MojoPipeline = _

  def getOrCreateModel() = {
    if (model == null) {
      val reader = MojoPipelineReaderBackendFactory.createReaderBackend(new ByteArrayInputStream(mojoData))
      model = MojoPipeline.loadFrom(reader)
    }
    model
  }

  def this(mojoData: Array[Byte]) = this(mojoData, Identifiable.randomUID("mojoPipelineModel"))

  val outputCol = "prediction"

  case class Mojo2Prediction(preds: List[String])
  private val modelUdf = (names: Array[String]) =>
    udf[Mojo2Prediction, Row] {
    r: Row =>
      val m = getOrCreateModel()
      val builder = m.getInputFrameBuilder
      val data = r.getValuesMap[Any](names).values.toArray.map(_.toString).zip(r.getValuesMap[Any](names).keys)
      val rowBuilder = builder.getMojoRowBuilder

      data.foreach{
        case (colData, colName) =>
          rowBuilder.setValue(colName, colData)
      }
      builder.addRow(rowBuilder)
      val output = m.transform(builder.toMojoFrame)
      val predictions = output.getColumnNames.zipWithIndex.map { case (_, i) =>
        val predictedRows =output.getColumnData(i).asInstanceOf[Array[_]]
        if (predictedRows.length != 1) {
          throw new RuntimeException("Invalid state, we predict on each row by row, independently at this moment.")
        }
        predictedRows(0).toString
      }

      Mojo2Prediction(predictions.toList)
  }

  def defaultFileName: String = H2OMojoPipelineModel.defaultFileName

  override def copy(extra: ParamMap): H2OMojoPipelineModel = defaultCopy(extra)

  override def write: MLWriter = new H2OMOJOPipelineModelWriter(this)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val flatten = H2OSchemaUtils.flattenDataFrame(dataset.toDF())
    val names = flatten.schema.fields.map(f => flatten(f.name))

    // get the altered frame
    flatten.select(col("*"), modelUdf(flatten.columns)(struct(names: _*)).as(outputCol))
  }

  def predictionSchema(): Seq[StructField] = {
    val fields = StructField("original", ArrayType(DoubleType)) :: Nil
    Seq(StructField(outputCol, StructType(fields), nullable = false))
  }

  override def transformSchema(schema: StructType): StructType = {
    StructType(schema ++ predictionSchema())
  }
}

private[models] class H2OMOJOPipelineModelWriter(instance: H2OMojoPipelineModel) extends MLWriter {

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

private[models] class H2OMOJOModelPipelineReader
(val defaultFileName: String) extends MLReader[H2OMojoPipelineModel] {

  private val className = implicitly[ClassTag[H2OMojoPipelineModel]].runtimeClass.getName

  @org.apache.spark.annotation.Since("1.6.0")
  override def load(path: String): H2OMojoPipelineModel = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
    val file = new File(path, defaultFileName)
    val is = new FileInputStream(file)
    val mojoData = Stream.continually(is.read).takeWhile(_ != -1).map(_.toByte).toArray

    val h2oModel = make(mojoData, metadata.uid)(sqlContext)
    DefaultParamsReader.getAndSetParams(h2oModel, metadata)
    h2oModel
  }

  def make(mojoData: Array[Byte], uid: String)(sqLContext: SQLContext): H2OMojoPipelineModel = {
    new H2OMojoPipelineModel(mojoData, uid)
  }
}


object H2OMojoPipelineModel extends MLReadable[H2OMojoPipelineModel] {
  val defaultFileName = "mojo_pipeline_model"

  @Since("1.6.0")
  override def read: MLReader[H2OMojoPipelineModel] = new H2OMOJOModelPipelineReader(defaultFileName)

  @Since("1.6.0")
  override def load(path: String): H2OMojoPipelineModel = super.load(path)

  def createFromMojo(path: String): H2OMojoPipelineModel = {
    val f = new File(path)
    createFromMojo(new FileInputStream(f), f.getName)
  }

  def createFromMojo(is: InputStream, uid: String = Identifiable.randomUID("mojoPipelineModel")): H2OMojoPipelineModel = {
    val mojoData = Stream.continually(is.read).takeWhile(_ != -1).map(_.toByte).toArray
    val sparkMojoModel = new H2OMojoPipelineModel(mojoData, uid)
    // Reconstruct state of Spark H2O MOJO transformer based on H2O's Pipeline Mojo
    sparkMojoModel
  }
}
