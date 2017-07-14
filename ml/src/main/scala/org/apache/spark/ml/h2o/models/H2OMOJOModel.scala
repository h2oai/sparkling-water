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

import hex.genmodel.{ModelMojoReader, MojoModel, MojoReaderBackendFactory}
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.ml.linalg.Vector
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Model => SparkModel}
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}

import scala.reflect.ClassTag

abstract class H2OMOJOModel[S <: H2OMOJOModel[S]]
                            (val model: MojoModel, val mojoData: Array[Byte], val sqlContext: SQLContext)
                            extends SparkModel[S] with MLWritable {

  var predictionCol: String = _
  var featuresCol: String = _

  /** @group getParam */
  final def getPredictionCol: String = predictionCol

  override def copy(extra: ParamMap): S = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    val predictUDF = udf { features: Vector => predict(features) }
    dataset.withColumn(predictionCol, predictUDF(col(featuresCol)))
  }

  def predict(features: Vector): Array[Double] = {
    val ncols: Int = if (model.nclasses() == 1) 1 else model.nclasses() + 1
    model.score0(features.toArray, new Array[Double](ncols))
  }

  /*  def predict(data: RowData): Row = {
      val wr = new EasyPredictModelWrapper(model)
      model.getModelCategory match {
         case ModelCategory.Binomial => Row(wr.predictBinomial(data).classProbabilities)
         case ModelCategory.Multinomial => Row(wr.predictMultinomial(data).classProbabilities)
         case ModelCategory.Regression => Row(wr.predictRegression(data).value)
         case ModelCategory.Clustering => Row(wr.predictClustering(data).cluster)
         case ModelCategory.AutoEncoder => throw new RuntimeException("UnImplemented")
         case ModelCategory.DimReduction => Row(wr.predictDimReduction(data).dimensions)
         case ModelCategory.WordEmbedding => Row(wr.predictWord2Vec(data).wordEmbeddings)
         case ModelCategory.Unknown => throw new RuntimeException("Unknown")
      }
    }*/

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val ncols: Int = if (model.nclasses() == 1) 1 else model.nclasses() + 1
    StructType(model.getNames.map {
      name => StructField(name, DoubleType, nullable = true, metadata = null)
    })
  }

  @Since("1.6.0")
  override def write: MLWriter = new H2OMOJOModelWriter[S](this.asInstanceOf[S])

  def defaultFileName: String
}

private[models] class H2OMOJOModelWriter[S <: H2OMOJOModel[S]](instance: S) extends MLWriter {

  @org.apache.spark.annotation.Since("1.6.0")
  override protected def saveImpl(path: String): Unit = {
    DefaultParamsWriter.saveMetadata(instance, path, sc)
    val file = new java.io.File(path, instance.defaultFileName)
    val fos = new FileOutputStream(file)
    fos.write(instance.mojoData)
  }
}

private[models] abstract class H2OMOJOModelReader[S <: H2OMOJOModel[S] : ClassTag]
(val defaultFileName: String) extends MLReader[S] {

  private val className = implicitly[ClassTag[S]].runtimeClass.getName

  @org.apache.spark.annotation.Since("1.6.0")
  override def load(path: String): S = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
    val file = new File(path, defaultFileName)
    val is = new FileInputStream(file)
    val mojoData = Stream.continually(is.read).takeWhile(_ != -1).map(_.toByte).toArray
    val reader = MojoReaderBackendFactory.createReaderBackend(is, MojoReaderBackendFactory.CachingStrategy.MEMORY)
    val model = ModelMojoReader.readFrom(reader)
    val h2oModel = make(model, mojoData, metadata.uid)(sqlContext)
    DefaultParamsReader.getAndSetParams(h2oModel, metadata)
    h2oModel
  }

  protected def make(model: MojoModel, mojoData: Array[Byte], uid: String)(sqLContext: SQLContext): S
}
