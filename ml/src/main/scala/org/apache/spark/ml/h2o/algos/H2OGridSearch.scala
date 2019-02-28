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

import hex.Model
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.glm.GLMModel.GLMParameters
import hex.grid.{Grid, GridSearch}
import hex.tree.gbm.GBMModel.GBMParameters
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.h2o._
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.h2o.models.H2OMOJOModel
import org.apache.spark.ml.h2o.param.{HyperParamsParam, NullableStringParam, ParametersParam}
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SQLContext}
import water.Key
import water.support.{H2OFrameSupport, ModelSerializationSupport}

import scala.collection.JavaConverters._
import scala.collection.mutable
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
    validateInput(params)

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

    params._response_column = getPredictionCol()
    val trainFrame = params._train.get()
    if (getAllStringColumnsToCategorical()) {
      H2OFrameSupport.allStringVecToCategorical(trainFrame)
    }
    H2OFrameSupport.columnsToCategorical(trainFrame, getColumnsToCategorical())

    water.DKV.put(trainFrame)
    val job = GridSearch.startGridSearch(Key.make(), params, hyperParams.asJava)
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

  private def validateInput(params: Model.Parameters): Unit ={
    if (getAlgo() == null) {
      throw new IllegalArgumentException(s"Algorithm has to be specified. Available algorithms are " +
        s"${H2OGridSearch.SupportedAlgos.allAsString}")
    }
    if (!H2OGridSearch.SupportedAlgos.isSupportedAlgo(getAlgo())) {
      throw new IllegalArgumentException(s"Grid Search is not supported for the specified algorithm '${getAlgo()}'. Supported " +
        s"algorithms are ${H2OGridSearch.SupportedAlgos.allAsString}")
    }
    H2OGridSearch.SupportedAlgos.fromString(getAlgo()).get match {
      case H2OGridSearch.SupportedAlgos.deeplearning => if(!params.isInstanceOf[DeepLearningParameters]) {
        throw new IllegalArgumentException(s"You specified algorithm '${getAlgo()}', but specified parameters for different algorithm." +
          s" Please make sure to use DeepLearningParameters")
      }
      case H2OGridSearch.SupportedAlgos.glm => if(!params.isInstanceOf[GLMParameters]) {
        throw new IllegalArgumentException(s"You specified algorithm '${getAlgo()}', but specified parameters for different algorithm." +
          s" Please make sure to use GLMParameters")
      }
      case H2OGridSearch.SupportedAlgos.gbm => if(!params.isInstanceOf[GBMParameters]) {
        throw new IllegalArgumentException(s"You specified algorithm '${getAlgo()}', but specified parameters for different algorithm." +
          s" Please make sure to use GBMParameters")
      }
    }

    if (params == null) {
      throw new IllegalArgumentException("Parameters for the Grid Search job have to be specified")
    }

  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  private var modelSelectionClosure: Option[Grid[_] => Model[_, _, _]] = None

  def setModelSelectionClosure(cl: Grid[_] => Model[_, _, _]) = {
    modelSelectionClosure = Some(cl)
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  @Since("1.6.0")
  override def write: MLWriter = new H2OGridSearchWriter(this)

  def defaultFileName: String = H2OGridSearch.defaultFileName
}


object H2OGridSearch extends MLReadable[H2OGridSearch] {

  object SupportedAlgos extends Enumeration {
    val gbm, glm, deeplearning = Value // still missing pipeline wrappers for KMeans & drf

    def isSupportedAlgo(s: String) = values.exists(_.toString == s.toLowerCase())

    def allAsString = values.mkString(", ")

    def fromString(value: String) = values.find(_.toString == value)
  }

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
    val outputPath = new Path(path, instance.defaultFileName)
    val fs = outputPath.getFileSystem(hadoopConf)
    val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val out = fs.create(qualifiedOutputPath)
    val oos = new ObjectOutputStream(out)
    oos.writeObject(instance.gridSearchParams.orNull)
    out.close()
    logInfo(s"Saved to: $qualifiedOutputPath")
  }
}

private[algos] class H2OGridSearchReader(val defaultFileName: String) extends MLReader[H2OGridSearch] {

  private val className = implicitly[ClassTag[H2OGridSearch]].runtimeClass.getName

  override def load(path: String): H2OGridSearch = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc, className)

    val inputPath = new Path(path, defaultFileName)
    val fs = inputPath.getFileSystem(sc.hadoopConfiguration)
    val qualifiedInputPath = inputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val ois = new ObjectInputStream(fs.open(qualifiedInputPath))

    val gridSearchParams = ois.readObject().asInstanceOf[H2OGridSearchParams]
    implicit val h2oContext: H2OContext = H2OContext.ensure("H2OContext has to be started in order to use H2O pipelines elements.")
    val algo = new H2OGridSearch(Option(gridSearchParams), metadata.uid)(h2oContext, sqlContext)
    metadata.getAndSetParams(algo)
    algo
  }
}

trait H2OGridSearchParams extends Params {
  private final val parametersGLM = new ParametersParam[GLMParameters](this, "parametersGLM", "Parameters for the GLM algorithm")
  private final val parametersGBM = new ParametersParam[GBMParameters](this, "parametersGBM", "Parameters for the GBM algorithm")
  private final val parametersDL = new ParametersParam[DeepLearningParameters](this, "parametersDL", "Parameters for the DL algorithm")

  //
  // Param definitions
  //
  private final val ratio = new DoubleParam(this, "ratio", "Determines in which ratios split the dataset")
  private final val algo = new NullableStringParam(this, "algo", "Specifies the algorithm for the GridSearch")
  private final val hyperParameters = new HyperParamsParam(this, "hyperParameters", "Hyper Parameters")
  private final val predictionCol = new NullableStringParam(this, "predictionCol", "Prediction column name")
  private final val allStringColumnsToCategorical = new BooleanParam(this, "allStringColumnsToCategorical", "Transform all strings columns to categorical")
  private final val columnsToCategorical = new StringArrayParam(this, "columnsToCategorical", "List of columns to convert to categoricals before modelling")
  //
  // Default values
  //
  setDefault(
    algo -> null,
    ratio -> 1.0, // 1.0 means use whole frame as training frame
    parametersGBM -> null,
    parametersDL -> null,
    parametersGLM -> null,
    hyperParameters -> Map.empty[String, Array[AnyRef]],
    predictionCol -> "prediction",
    allStringColumnsToCategorical -> true,
    columnsToCategorical -> Array.empty[String]
  )

  //
  // Getters
  //
  /** @group getParam */
  def getRatio() = $(ratio)

  /** @group getParam */
  def getAlgo() = $(algo)

  /** @group getParam */
  def getParameters() = {
    Seq($(parametersGLM), $(parametersGBM), $(parametersDL)).find(_ != null).get
  }

  /** @group getParam */
  def getHyperParameters() = $(hyperParameters)

  /** @group getParam */
  def getPredictionCol() = $(predictionCol)

  /** @group getParam */
  def getAllStringColumnsToCategorical() = $(allStringColumnsToCategorical)

  /** @group getParam */
  def getColumnsToCategorical() = $(columnsToCategorical)

  //
  // Setters
  //
  /** @group setParam */
  def setRatio(value: Double): this.type = set(ratio, value)

  /** @group setParam */
  def setAlgo(value: String): this.type = {
    if (value == null) {
      throw new IllegalArgumentException(s"Algorithm value can't be null. " +
        s"Supported algorithms are ${H2OGridSearch.SupportedAlgos.allAsString}")
    }

    if (!H2OGridSearch.SupportedAlgos.isSupportedAlgo(value)) {
      throw new IllegalArgumentException(s"Grid Search is not supported for the specified algorithm '$value'. Supported " +
        s"algorithms are ${H2OGridSearch.SupportedAlgos.allAsString}")
    }
    set(algo, value)
  }

  /** @group setParam */
  def setParameters[P <: Model.Parameters](value: P)(implicit tag: ClassTag[P]): this.type = {
    value match {
      case _: GLMParameters =>
        set(parametersGBM, null)
        set(parametersDL, null)
        set(parametersGLM, value.asInstanceOf[GLMParameters])
      case _: GBMParameters =>
        set(parametersGLM, null)
        set(parametersDL, null)
        set(parametersGBM, value.asInstanceOf[GBMParameters])
      case _: DeepLearningParameters =>
        set(parametersGBM, null)
        set(parametersGLM, null)
        set(parametersDL, value.asInstanceOf[DeepLearningParameters])
      case _ => throw new IllegalArgumentException("Grid Search is not supported for the specified algorithm. Supported" +
        s"algorithms are ${H2OGridSearch.SupportedAlgos.allAsString}")
    }
  }


  /** @group getParam */
  def setHyperParameters(value: Map[String, Array[AnyRef]]): this.type = set(hyperParameters, value)

  /** @group getParam */
  def setHyperParameters(value: mutable.Map[String, Array[AnyRef]]): this.type = set(hyperParameters, value.toMap)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setAllStringColumnsToCategorical(value: Boolean): this.type = set(allStringColumnsToCategorical, value)

  /** @group setParam */
  def setColumnsToCategorical(first: String, others: String*): this.type = set(columnsToCategorical, Array(first) ++ others)

  /** @group setParam */
  def setColumnsToCategorical(columns: Array[String]): this.type = set(columnsToCategorical, columns)
}


