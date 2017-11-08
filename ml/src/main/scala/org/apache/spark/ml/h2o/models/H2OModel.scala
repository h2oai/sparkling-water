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

import java.io.File

import hex.Model
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.h2o.{H2OContext, H2OFrame}
import org.apache.spark.ml.h2o.param.H2OModelParams
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Model => SparkModel}
import org.apache.spark.sql.types.{DoubleType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, SQLContext}
import water.support.ModelSerializationSupport

import scala.reflect.ClassTag
import scala.util.Random

/**
  * Shared implementation for H2O model pipelines
  */
abstract class H2OModel[S <: H2OModel[S, M],
M <: Model[_, _, _ <: Model.Output]]
(val model: M, h2oContext: H2OContext, sqlContext: SQLContext)
  extends SparkModel[S] with H2OModelParams with MLWritable {


  override def copy(extra: ParamMap): S = defaultCopy(extra)

  override def transform(dataset: Dataset[_]): DataFrame = {
    // in order to join the predictions with the original data we do a small trick
    // we attach unique id to each row and then convert to H2O Frame
    // we do predictions on the frame without ids but and add the ids to the predicted values afterwards
    // this is save as h2o preserves the order.

    // next we convert back to spark and join the original dataframe with the predicted values
    val tempId = Random.alphanumeric.take(20).mkString("")
    import org.apache.spark.sql.functions._
    val datasetWithId = dataset.withColumn(tempId, monotonically_increasing_id)


    import org.apache.spark.sql.functions.col
    val cols = ($(featuresCols) ++ Array(tempId)).map(col)


    val frame: H2OFrame = h2oContext.asH2OFrame(datasetWithId.select(cols: _*).toDF())

    val ids = frame.extractFrame(frame._names.length - 1, frame._names.length)

    // H2O Preserves the Order
    val frameNoId = frame.extractFrame(0, frame._names.length - 1) // don't take the last column
    val prediction = model.score(frameNoId)
    val predictionIds = prediction.add(ids)
    predictionIds.update()
    val predictionsSpark = h2oContext.asDataFrame(predictionIds)(sqlContext)

    datasetWithId.join(predictionsSpark, tempId).drop(tempId)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val ncols: Int = if (model._output.nclasses() == 1) 1 else model._output.nclasses() + 1

    StructType(schema.fields ++ model._output._names.map {
      name => StructField(name, DoubleType, nullable = true, metadata = null)
    })
  }

  @Since("1.6.0")
  override def write: MLWriter = new H2OModelWriter[S](this.asInstanceOf[S])

  def defaultFileName: String
}

private[models] class H2OModelWriter[T <: H2OModel[T, _ <: Model[_, _, _ <: Model.Output]]](instance: T) extends MLWriter {

  @org.apache.spark.annotation.Since("1.6.0")
  override protected def saveImpl(path: String): Unit = {
    DefaultParamsWriter.saveMetadata(instance, path, sc)
    val file = new java.io.File(path, instance.defaultFileName)

    ModelSerializationSupport.exportH2OModel(instance.model, file.toURI)
  }
}

private[models] abstract class H2OModelReader[T <: H2OModel[T, M] : ClassTag, M <: Model[_, _, _ <: Model.Output]]
(val defaultFileName: String) extends MLReader[T] {

  private val className = implicitly[ClassTag[T]].runtimeClass.getName

  @org.apache.spark.annotation.Since("1.6.0")
  override def load(path: String): T = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
    val file = new File(path, defaultFileName)
    val model = ModelSerializationSupport.loadH2OModel[M](file.toURI)
    implicit val h2oContext = H2OContext.ensure("H2OContext has to be started in order to use H2O pipelines elements")
    val h2oModel = make(model, metadata.uid)(h2oContext, sqlContext)
    DefaultParamsReader.getAndSetParams(h2oModel, metadata)
    h2oModel
  }

  protected def make(model: M, uid: String)(implicit h2oContext: H2OContext, sqLContext: SQLContext): T
}
