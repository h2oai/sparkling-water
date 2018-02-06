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
package org.apache.spark.ml.h2o.algos

import java.io._
import java.util.Date

import ai.h2o.automl.{AutoML, AutoMLBuildSpec}
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.schemas.DeepLearningV3.DeepLearningParametersV3
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.h2o._
import org.apache.spark.ml.h2o.models.H2OMOJOModel
import org.apache.spark.ml.h2o.param.H2OAlgoParams
import org.apache.spark.ml.param.{Param, ParamMap, Params}
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model => SparkModel}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SQLContext}
import water.Key
import water.support.{H2OFrameSupport, ModelSerializationSupport}

import scala.reflect.ClassTag

/**
  * Base class for H2O algorithm wrapper as a Spark transformer.
  */
class H2OAutoML
(val automlBuildSpec: Option[AutoMLBuildSpec],  override val uid: String)
(implicit hc: H2OContext, sqlContext: SQLContext)
  extends Estimator[H2OMOJOModel] with MLWritable with H2OAutoMLParams {

  def this()(implicit hc: H2OContext, sqlContext: SQLContext) = this(None, Identifiable.randomUID("automl"))

  override def fit(dataset: Dataset[_]): H2OMOJOModel = {
    val spec = automlBuildSpec.getOrElse(new AutoMLBuildSpec)
    val trainFrame = hc.asH2OFrame(dataset.toDF())
    H2OFrameSupport.allStringVecToCategorical(trainFrame)
    spec.input_spec.training_frame = trainFrame._key
    spec.input_spec.response_column = getPredictionCol()
    water.DKV.put(trainFrame)
    val aml = new AutoML(Key.make(uid), new Date(), spec)
    AutoML.startAutoML(aml)
    // Block until AutoML finishes
    aml.get()
    val model = new H2OMOJOModel(ModelSerializationSupport.getMojoData(aml.leader()))

    model
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  @Since("1.6.0")
  override def write: MLWriter = new H2OAutoMLWriter(this)

  def defaultFileName: String = H2OAutoML.defaultFileName
}


object H2OAutoML extends MLReadable[H2OAutoML] {

  private final val defaultFileName = "automl_params"

  @Since("1.6.0")
  override def read: MLReader[H2OAutoML] = new H2OAutoMLReader(defaultFileName)

  @Since("1.6.0")
  override def load(path: String): H2OAutoML = super.load(path)
}

// FIXME: H2O Params are iced objects!
private[algos] class H2OAutoMLWriter(instance: H2OAutoML) extends MLWriter {

  @Since("1.6.0") override protected def saveImpl(path: String): Unit = {
    val hadoopConf = sc.hadoopConfiguration
    DefaultParamsWriter.saveMetadata(instance, path, sc)
    val outputPath = if (path.startsWith("file://")) {
      new Path(path, instance.defaultFileName)
    } else {
      new Path("file://" + path, instance.defaultFileName)
    }
    val fs = outputPath.getFileSystem(hadoopConf)
    val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    fs.create(qualifiedOutputPath)
    val oos = new ObjectOutputStream(new FileOutputStream(new File(qualifiedOutputPath.toUri), false))
    oos.writeObject(instance.automlBuildSpec.get)
  }
}

private[algos] class H2OAutoMLReader(val defaultFileName: String) extends MLReader[H2OAutoML] {

  private val className = implicitly[ClassTag[H2OAutoML]].runtimeClass.getName

  override def load(path: String): H2OAutoML = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
    val file = new File(path, defaultFileName)
    val ois = new ObjectInputStream(new FileInputStream(file))
    val buildSpec = ois.readObject().asInstanceOf[AutoMLBuildSpec]
    implicit val h2oContext: H2OContext = H2OContext.ensure("H2OContext has to be started in order to use H2O pipelines elements.")
    new H2OAutoML(Some(buildSpec), metadata.uid)(h2oContext, sqlContext)
  }
}

trait H2OAutoMLParams extends Params {

  //
  // Param definitions
  //
  private final val predictionCol = new Param[String](this, "predictionCol", "Prediction column name")
  //
  // Default values
  //
  setDefault(
    predictionCol -> "prediction"
  )

  //
  // Getters
  //
  /** @group getParam */
  def getPredictionCol() = $(predictionCol)
  //
  // Setters
  //
  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

}
