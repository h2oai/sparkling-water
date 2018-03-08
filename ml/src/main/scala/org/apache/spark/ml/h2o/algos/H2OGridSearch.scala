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
import java.util

import hex.grid.{Grid, GridSearch}
import hex.tree.gbm.GBMModel.GBMParameters
import hex.{Model, ScoreKeeper}
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.h2o._
import org.apache.spark.ml.h2o.models.H2OMOJOModel
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.ml.{Estimator, Model => SparkModel}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SQLContext}
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{JNull, JString, JValue}
import water.Key
import water.support.{H2OFrameSupport, ModelSerializationSupport}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
  * H2O Grid Search, currently available just for GBM
  */
class H2OGridSearch(val gridSearchParams: Option[H2OGridSearchParams], override val uid: String)
                   (implicit hc: H2OContext, sqlContext: SQLContext)
  extends Estimator[H2OMOJOModel] with MLWritable with H2OGridSearchParams {

  def this()(implicit hc: H2OContext, sqlContext: SQLContext) = this(None, Identifiable.randomUID("gridsearch"))

  def this(uid: String, hc: H2OContext, sqlContext: SQLContext) = this(None, uid)(hc, sqlContext)

  // Currently, we support only GBM in grid search, we can safely return H2OMojoModel
  override def fit(dataset: Dataset[_]): H2OMOJOModel = {
    val params = gridSearchParams.map(_.getParameters()).getOrElse(getParameters())
    val hyperParams = gridSearchParams.map(_.getHyperParameters()).getOrElse(getHyperParameters())
    val input = hc.asH2OFrame(dataset.toDF())
    // check if we need to do any splitting
    if (getRatio() < 1.0) {
      // need to do splitting
      val keys = H2OFrameSupport.split(input, Seq(Key.rand(), Key.rand()), Seq(getRatio()))
      params._train = keys(0)._key
      if (keys.length > 1) {
        params._valid = keys(1)._key
      }
    } else {
      params._train = input._key
    }

    params._response_column = getPredictionsCol()
    val trainFrame = params._train.get()
    if (getAllStringColumnsToCategorical()) {
      H2OFrameSupport.allStringVecToCategorical(trainFrame)
    }

    water.DKV.put(trainFrame)
    val job = GridSearch.startGridSearch(Key.make(), params, hyperParams)
    val grid = job.get()
    if (grid.getModels.length == 0) {
      throw new IllegalArgumentException("No Model returned.")
    }
    val modelFromGrid = if (modelSelectionClosure.isEmpty) {
      grid.getModels()(0)
    } else {
      modelSelectionClosure.get.apply(grid)
    }
    // Block until GridSearch finishes
    val model = new H2OMOJOModel(ModelSerializationSupport.getMojoData(modelFromGrid))
    model.setConvertUnknownCategoricalLevelsToNa(true)
    model
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  private var modelSelectionClosure: Option[Grid[_] => Model[_, _, _]] = None

  def setModelSelectionClosure(cl: (Grid[_]) => Model[_, _, _]) = {
    modelSelectionClosure = Some(cl)
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  @Since("1.6.0")
  override def write: MLWriter = new H2OGridSearchWriter(this)

  def defaultFileName: String = H2OGridSearch.defaultFileName
}


object H2OGridSearch extends MLReadable[H2OGridSearch] {

  private final val defaultFileName = "gridsearch_params"

  @Since("1.6.0")
  override def read: MLReader[H2OGridSearch] = new H2OGridSearchReader(defaultFileName)

  @Since("1.6.0")
  override def load(path: String): H2OGridSearch = super.load(path)
}

// FIXME: H2O Params are iced objects!
private[algos] class H2OGridSearchWriter(instance: H2OGridSearch) extends MLWriter {

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
    oos.writeObject(instance.gridSearchParams.get)
  }
}

private[algos] class H2OGridSearchReader(val defaultFileName: String) extends MLReader[H2OGridSearch] {

  private val className = implicitly[ClassTag[H2OGridSearch]].runtimeClass.getName

  override def load(path: String): H2OGridSearch = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc, className)
    val file = new File(path, defaultFileName)
    val ois = new ObjectInputStream(new FileInputStream(file))
    val gridSearchParams = ois.readObject().asInstanceOf[H2OGridSearchParams]
    implicit val h2oContext: H2OContext = H2OContext.ensure("H2OContext has to be started in order to use H2O pipelines elements.")
    new H2OGridSearch(Some(gridSearchParams), metadata.uid)(h2oContext, sqlContext)
  }
}

trait H2OGridSearchParams extends Params {

  //
  // Param definitions
  //
  private final val ratio = new DoubleParam(this, "ratio", "Determines in which ratios split the dataset")
  private final val algo = new Param[String](this, "algo", "Specifies the algorithm for the GridSearch")
  private final val parameters = new Param[GBMParameters](this, "parameters", "Parameters for the algorithm")
  private final val hyperParameters = new Param[util.Map[String, Array[AnyRef]]](this, "hyperParameters", "Hyper Parameters")
  private final val predictionCol = new Param[String](this, "predictionCol", "Prediction column name")
  private final val allStringColumnsToCategorical = new BooleanParam(this, "allStringColumnsToCategorical", "Transform all strings columns to categorical")
  //
  // Default values
  //
  setDefault(
    ratio -> 1.0, // 1.0 means use whole frame as training frame
    algo -> "GBM",
    parameters -> new GBMParameters(),
    hyperParameters -> Map.empty[String, Array[AnyRef]].asJava,
    predictionCol -> "prediction",
    allStringColumnsToCategorical -> true
  )

  //
  // Getters
  //
  /** @group getParam */
  def getRatio() = $(ratio)

  /** @group getParam */
  def getAlgo() = $(algo)

  /** @group getParam */
  def getParameters() = $(parameters)

  /** @group getParam */
  def getHyperParameters() = $(hyperParameters)

  /** @group getParam */
  def getPredictionsCol() = $(predictionCol)

  /** @group getParam */
  def getAllStringColumnsToCategorical() = $(allStringColumnsToCategorical)

  //
  // Setters
  //
  /** @group setParam */
  def setRatio(value: Double): this.type = set(ratio, value)

  /** @group setParam */
  def setAlgo(value: String): this.type = set(algo, value)

  /** @group setParam */
  def setParameters(value: GBMParameters): this.type = set(parameters, value)

  /** @group setParam */
  def setParameters(value: H2OGBM): this.type = set(parameters, value.getParams)

  /** @group getParam */
  def setHyperParameters(value: util.Map[String, Array[AnyRef]]): this.type = set(hyperParameters, value)

  /** @group setParam */
  def setPredictionsCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setAllStringColumnsToCategorical(value: Boolean): this.type = set(allStringColumnsToCategorical, value)

}

class H2OGridSearchAlgoParameters private(parent: Params, name: String, doc: String, isValid: ScoreKeeper.StoppingMetric => Boolean)
  extends Param[ScoreKeeper.StoppingMetric](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: ScoreKeeper.StoppingMetric): ParamPair[ScoreKeeper.StoppingMetric] = super.w(value)

  override def jsonEncode(value: ScoreKeeper.StoppingMetric): String = {
    val encoded: JValue = if (value == null) {
      JNull
    } else {
      JString(value.toString)
    }
    compact(render(encoded))
  }

  override def jsonDecode(json: String): ScoreKeeper.StoppingMetric = {
    val parsed = parse(json)
    parsed match {
      case JString(x) =>
        ScoreKeeper.StoppingMetric.valueOf(x)
      case JNull =>
        null
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $parsed to ScoreKeeper.StoppingMetric.")
    }
  }
}
