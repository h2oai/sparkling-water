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
import java.lang.reflect.Field
import java.util

import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.glm.GLMModel.GLMParameters
import hex.grid.GridSearch.SimpleParametersBuilderFactory
import hex.grid.HyperSpaceSearchCriteria.{CartesianSearchCriteria, RandomDiscreteValueSearchCriteria}
import hex.{Model, ScoreKeeper}
import hex.grid.{Grid, GridSearch, HyperSpaceSearchCriteria}
import hex.tree.gbm.GBMModel.GBMParameters
import hex.tree.xgboost.XGBoostModel.XGBoostParameters
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.h2o._
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.h2o.models.H2OMOJOModel
import org.apache.spark.ml.h2o.param._
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

  override def fit(dataset: Dataset[_]): H2OMOJOModel = {
    val algoParams = gridSearchParams.map(_.getAlgoParams()).getOrElse(getAlgoParams())

    if (algoParams == null) {
      throw new IllegalArgumentException(s"Algorithm has to be specified. Available algorithms are " +
        s"${H2OGridSearch.SupportedAlgos.allAsString}")
    }

    if (!H2OGridSearch.SupportedAlgos.isSupportedAlgo(algoParams.algoName())) {
      throw new IllegalArgumentException(s"Grid Search is not supported for the specified algorithm '${algoParams.algoName()}'. Supported " +
        s"algorithms are ${H2OGridSearch.SupportedAlgos.allAsString}")
    }

    val hyperParams = processHyperParams(algoParams, gridSearchParams.map(_.getHyperParameters()).getOrElse(getHyperParameters()))

    val input = hc.asH2OFrame(dataset.toDF())
    // check if we need to do any splitting
    if (getRatio() < 1.0) {
      // need to do splitting
      val keys = H2OFrameSupport.split(input, Seq(Key.rand(), Key.rand()), Seq(getRatio()))
      algoParams._train = keys(0)._key
      if (keys.length > 1) {
        algoParams._valid = keys(1)._key
      }
    } else {
      algoParams._train = input._key
    }

    algoParams._response_column = getPredictionCol()
    val trainFrame = algoParams._train.get()
    if (getAllStringColumnsToCategorical()) {
      H2OFrameSupport.allStringVecToCategorical(trainFrame)
    }
    H2OFrameSupport.columnsToCategorical(trainFrame, getColumnsToCategorical())

    water.DKV.put(trainFrame)
    val criteria = getStrategy() match {
      case HyperSpaceSearchCriteria.Strategy.Cartesian => new CartesianSearchCriteria
      case HyperSpaceSearchCriteria.Strategy.RandomDiscrete =>
        val c = new RandomDiscreteValueSearchCriteria
        c.set_stopping_tolerance(getStoppingTolerance)
        c.set_stopping_rounds(getStoppingRounds)
        c.set_stopping_metric(getStoppingMetric)
        c.set_seed(getSeed)
        c.set_max_models(getMaxModels)
        c.set_max_runtime_secs(getMaxRuntimeSecs())
        c
      case _ => new CartesianSearchCriteria
    }

    val paramsBuilder = algoParams match {

      case _ : GBMParameters => new SimpleParametersBuilderFactory[GBMParameters]
      case _ : DeepLearningParameters => new SimpleParametersBuilderFactory[DeepLearningParameters]
      case _ : GLMParameters => new SimpleParametersBuilderFactory[GLMParameters]
      case _ : XGBoostParameters => new SimpleParametersBuilderFactory[XGBoostParameters]
      case algo => throw new IllegalArgumentException("Unsupported Algorithm " + algo.algoName())
    }
    val job = GridSearch.startGridSearch(Key.make(), algoParams, hyperParams,
      paramsBuilder.asInstanceOf[SimpleParametersBuilderFactory[Model.Parameters]], criteria)
    val grid = job.get()

    // Block until GridSearch finishes
    val model = trainModel(grid)
    model.setConvertUnknownCategoricalLevelsToNa(true)
    model
  }

  //noinspection ComparingUnrelatedTypes
  private def processHyperParams(params: Model.Parameters, hyperParams: java.util.Map[String, Array[AnyRef]]) = {
    // If we use PySparkling, we don't have information whether the type is long or int and to set the hyper parameters
    // correctly, we need this information. We therefore check the type of the hyper parameter at run-time and try to set it

    // we only need to worry about distinguishing between int and long
    val checkedHyperParams = new java.util.HashMap[String, Array[AnyRef]]()

    val it = hyperParams.entrySet().iterator()
    while (it.hasNext) {
      val entry = it.next()
      val hyperParamName = entry.getKey

      val hyperParamValues = if (entry.getValue.isInstanceOf[util.ArrayList[Object]]) {
        val length = entry.getValue.asInstanceOf[util.ArrayList[_]].size()
        val arrayList = entry.getValue.asInstanceOf[util.ArrayList[_]]
        (0 until length).map(idx => arrayList.get(idx).asInstanceOf[AnyRef]).toArray
      } else {
        entry.getValue
      }

      // get automatically box the java primitives so we can work with them
      val convertedValue = findField(params, hyperParamName).get(params) match {
        case v if v.isInstanceOf[java.lang.Long] =>
          hyperParamValues.map {
            case arrVal: java.lang.Long => arrVal.asInstanceOf[Long].longValue().asInstanceOf[AnyRef]
            case arrVal: java.lang.Integer => arrVal.asInstanceOf[Integer].longValue().asInstanceOf[AnyRef]
          }
        case _ => hyperParamValues
      }

      checkedHyperParams.put(hyperParamName, convertedValue)
    }
    checkedHyperParams
  }

  private def findField(params: Model.Parameters, hyperParamName: String): Field = {
    try {
      params.getClass.getField(hyperParamName)
    } catch {
      case _: NoSuchElementException => throw new IllegalArgumentException(s"No such parameter: '$hyperParamName'")
    }
  }

  def trainModel(grid: Grid[_]) = {
    new H2OMOJOModel(ModelSerializationSupport.getMojoData(selectModelFromGrid(grid)))
  }

  def selectModelFromGrid(grid: Grid[_]) = {
    if (grid.getModels.length == 0) {
      throw new IllegalArgumentException("No Model returned.")
    }
    if (modelSelectionClosure.isEmpty) {
      grid.getModels()(0)
    } else {
      modelSelectionClosure.get.apply(grid)
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

    def fromString(value: String) = values.find(_.toString == value.toLowerCase())
  }

  final val defaultFileName = "grid_search_params"

  @Since("1.6.0")
  override def read: MLReader[H2OGridSearch] = H2OGridSearchReader.create[H2OGridSearch](defaultFileName)

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

private[algos] class H2OGridSearchReader[A <: H2OGridSearch : ClassTag](val defaultFileName: String) extends MLReader[A] {

  override def load(path: String): A = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc)

    val inputPath = new Path(path, defaultFileName)
    val fs = inputPath.getFileSystem(sc.hadoopConfiguration)
    val qualifiedInputPath = inputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val ois = new ObjectInputStream(fs.open(qualifiedInputPath))

    val gridSearchParams = ois.readObject().asInstanceOf[H2OGridSearchParams]
    implicit val h2oContext: H2OContext = H2OContext.ensure("H2OContext has to be started in order to use H2O pipelines elements.")
    val algo = make[A](Option(gridSearchParams), metadata.uid, h2oContext, sqlContext)
    DefaultParamsReader.getAndSetParams(algo, metadata)
    algo
  }

  private def make[CT: ClassTag]
  (gridSearchParams: Option[H2OGridSearchParams], uid: String, h2oContext: H2OContext, sqlContext: SQLContext): CT = {
    val aClass = implicitly[ClassTag[CT]].runtimeClass
    val ctor = aClass.getConstructor(classOf[Option[H2OGridSearchParams]], classOf[String], classOf[H2OContext], classOf[SQLContext])
    ctor.newInstance(gridSearchParams, uid, h2oContext, sqlContext).asInstanceOf[CT]
  }
}


object H2OGridSearchReader {
  def create[A <: H2OGridSearch : ClassTag](defaultFileName: String) = new H2OGridSearchReader[A](defaultFileName)
}

trait H2OGridSearchParams extends Params {


  //
  // Param definitions
  //
  private final val algo = new DoubleParam(this, "algo", "dummy argument for pysparkling")
  private final val ratio = new DoubleParam(this, "ratio", "Determines in which ratios split the dataset")
  private final val algoParams = new AlgoParams(this, "algoParams", "Specifies the algorithm for grid search")
  private final val hyperParameters = new HyperParamsParam(this, "hyperParameters", "Hyper Parameters")
  private final val predictionCol = new NullableStringParam(this, "predictionCol", "Prediction column name")
  private final val allStringColumnsToCategorical = new BooleanParam(this, "allStringColumnsToCategorical", "Transform all strings columns to categorical")
  private final val columnsToCategorical = new StringArrayParam(this, "columnsToCategorical", "List of columns to convert to categoricals before modelling")
  private final val strategy = new GridSearchStrategyParam(this, "strategy", "Search criteria strategy")
  private final val maxRuntimeSecs = new DoubleParam(this, "maxRuntimeSecs", "maxRuntimeSecs")
  private final val maxModels = new IntParam(this, "maxModels", "maxModels")
  private final val seed = new LongParam(this, "seed", "seed for hyper params search")
  private final val stoppingRounds = new IntParam(this, "stoppingRounds", "Early stopping based on convergence of stoppingMetric")
  private final val stoppingTolerance = new DoubleParam(this, "stoppingTolerance", "Relative tolerance for metric-based" +
    " stopping criterion: stop if relative improvement is not at least this much.")
  private final val stoppingMetric = new StoppingMetricParam(this, "stoppingMetric", "Stopping Metric")


  //
  // Default values
  //
  setDefault(
    algoParams -> null,
    ratio -> 1.0, // 1.0 means use whole frame as training frame
    hyperParameters -> Map.empty[String, Array[AnyRef]].asJava,
    predictionCol -> "prediction",
    allStringColumnsToCategorical -> true,
    columnsToCategorical -> Array.empty[String],
    strategy -> HyperSpaceSearchCriteria.Strategy.Cartesian,
    maxRuntimeSecs -> 0,
    maxModels -> 0,
    seed -> -1,
    stoppingRounds -> 0,
    stoppingTolerance -> 0.001,
    stoppingMetric -> ScoreKeeper.StoppingMetric.AUTO
  )

  //
  // Getters
  //
  /** @group getParam */
  def getRatio() = $(ratio)

  /** @group getParam */
  def getAlgoParams() = $(algoParams)

  /** @group getParam */
  def getHyperParameters() = $(hyperParameters)

  /** @group getParam */
  def getPredictionCol() = $(predictionCol)

  /** @group getParam */
  def getAllStringColumnsToCategorical() = $(allStringColumnsToCategorical)

  /** @group getParam */
  def getColumnsToCategorical() = $(columnsToCategorical)

  /** @group getParam */
  def getStrategy() = $(strategy)

  /** @group getParam */
  def getMaxRuntimeSecs() = $(maxRuntimeSecs)

  /** @group getParam */
  def getMaxModels = $(maxModels)

  /** @group getParam */
  def getSeed = $(seed)

  /** @group getParam */
  def getStoppingRounds = $(stoppingRounds)

  /** @group getParam */
  def getStoppingTolerance = $(stoppingTolerance)

  /** @group getParam */
  def getStoppingMetric = $(stoppingMetric)

  //
  // Setters
  //
  /** @group setParam */
  def setRatio(value: Double): this.type = set(ratio, value)

  /** @group setParam */
  def setAlgo(value: H2OAlgorithm[_ <: Model.Parameters, _]): this.type = {
    if (!H2OGridSearch.SupportedAlgos.isSupportedAlgo(value.getParams.algoName())) {
      throw new IllegalArgumentException(s"Grid Search is not supported for the specified algorithm '$value'. Supported " +
        s"algorithms are ${H2OGridSearch.SupportedAlgos.allAsString}")
    }
    set(algoParams, value.getParams)
  }

  /** @group getParam */
  def setHyperParameters(value: Map[String, Array[AnyRef]]): this.type = set(hyperParameters, value.asJava)

  /** @group getParam */
  def setHyperParameters(value: mutable.Map[String, Array[AnyRef]]): this.type = set(hyperParameters, value.toMap.asJava)

  /** @group getParam */
  def setHyperParameters(value: java.util.Map[String, Array[AnyRef]]): this.type = set(hyperParameters, value)

  /** @group setParam */
  def setPredictionCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setAllStringColumnsToCategorical(value: Boolean): this.type = set(allStringColumnsToCategorical, value)

  /** @group setParam */
  def setColumnsToCategorical(first: String, others: String*): this.type = set(columnsToCategorical, Array(first) ++ others)

  /** @group setParam */
  def setColumnsToCategorical(columns: Array[String]): this.type = set(columnsToCategorical, columns)

  /** @group setParam */
  def setStrategy(value: HyperSpaceSearchCriteria.Strategy) = set(strategy, value)

  /** @group setParam */
  def setMaxRuntimeSecs(value: Double) = set(maxRuntimeSecs, value)

  /** @group setParam */
  def setMaxModels(value: Int) = set(maxModels, value)

  /** @group setParam */
  def setSeed(value: Long) = set(seed, value)

  /** @group setParam */
  def setStoppingRounds(value: Int) = set(stoppingRounds, value)

  /** @group setParam */
  def setStoppingTolerance(value: Double) = set(stoppingTolerance, value)

  /** @group getParam */
  def setStoppingMetric(value: ScoreKeeper.StoppingMetric) = set(stoppingMetric, value)
}

class GridSearchStrategyParam private[h2o](parent: Params, name: String, doc: String,
                                           isValid: HyperSpaceSearchCriteria.Strategy => Boolean)
  extends EnumParam[HyperSpaceSearchCriteria.Strategy](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)
}

