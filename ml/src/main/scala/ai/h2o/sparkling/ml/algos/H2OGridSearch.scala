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
package ai.h2o.sparkling.ml.algos

import java.lang.reflect.Field
import java.util

import ai.h2o.sparkling.backend.external.RestApiUtils
import ai.h2o.sparkling.ml.models.{H2OMOJOModel, H2OMOJOSettings}
import ai.h2o.sparkling.ml.params.{AlgoParam, H2OAlgoParamsHelper, H2OCommonSupervisedParams, HyperParamsParam}
import ai.h2o.sparkling.ml.utils.H2OParamsReadable
import ai.h2o.sparkling.model.{H2OMetric, H2OModel, H2OModelCategory}
import ai.h2o.sparkling.utils.SparkSessionUtils
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.glm.GLMModel.GLMParameters
import hex.grid.GridSearch.SimpleParametersBuilderFactory
import hex.grid.HyperSpaceSearchCriteria.{CartesianSearchCriteria, RandomDiscreteValueSearchCriteria}
import hex.grid.{Grid, GridSearch, HyperSpaceSearchCriteria}
import hex.tree.drf.DRFModel.DRFParameters
import hex.tree.gbm.GBMModel.GBMParameters
import hex.tree.xgboost.XGBoostModel.XGBoostParameters
import hex.{Model, ScoreKeeper}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.h2o._
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import water.support.ModelSerializationSupport
import water.util.PojoUtils
import water.{DKV, Key}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * H2O Grid Search algorithm exposed via Spark ML pipelines.
  *
  */
class H2OGridSearch(override val uid: String) extends Estimator[H2OMOJOModel]
  with H2OAlgoCommonUtils with DefaultParamsWritable with H2OGridSearchParams {

  def this() = this(Identifiable.randomUID(classOf[H2OGridSearch].getSimpleName))

  private var gridModels: Array[H2OModel] = _
  private var gridMojoModels: Array[H2OMOJOModel] = _

  private def fitOverClient(algoParams: Model.Parameters,
                            hyperParams: util.HashMap[String, Array[AnyRef]],
                            trainKey: String,
                            validKey: Option[String]): Array[Byte] = {
    algoParams._train = DKV.getGet[Frame](trainKey)._key
    algoParams._valid = validKey.map(DKV.getGet[Frame](_)._key).orNull

    algoParams._nfolds = getNfolds()
    algoParams._fold_column = getFoldCol()
    algoParams._response_column = getLabelCol()
    algoParams._weights_column = getWeightCol()
    val trainFrame = algoParams._train.get()

    water.DKV.put(trainFrame)
    val Cartesian = HyperSpaceSearchCriteria.Strategy.Cartesian.name()
    val RandomDiscrete = HyperSpaceSearchCriteria.Strategy.RandomDiscrete.name()
    val criteria = getStrategy() match {
      case Cartesian => new CartesianSearchCriteria
      case RandomDiscrete =>
        val c = new RandomDiscreteValueSearchCriteria
        c.set_stopping_tolerance(getStoppingTolerance())
        c.set_stopping_rounds(getStoppingRounds())
        c.set_stopping_metric(ScoreKeeper.StoppingMetric.valueOf(getStoppingMetric()))
        c.set_seed(getSeed())
        c.set_max_models(getMaxModels())
        c.set_max_runtime_secs(getMaxRuntimeSecs())
        c
      case _ => new CartesianSearchCriteria
    }

    val paramsBuilder = algoParams match {
      case _: GBMParameters => new SimpleParametersBuilderFactory[GBMParameters]
      case _: DeepLearningParameters => new SimpleParametersBuilderFactory[DeepLearningParameters]
      case _: GLMParameters => new SimpleParametersBuilderFactory[GLMParameters]
      case _: XGBoostParameters => new SimpleParametersBuilderFactory[XGBoostParameters]
      case _: DRFParameters => new SimpleParametersBuilderFactory[DRFParameters]
      case algo => throw new IllegalArgumentException("Unsupported Algorithm " + algo.algoName())
    }
    val job = GridSearch.startGridSearch(Key.make(), algoParams, hyperParams,
      paramsBuilder.asInstanceOf[SimpleParametersBuilderFactory[Model.Parameters]], criteria, GridSearch.getParallelismLevel(getParallelism()))
    // Block until GridSearch finishes
    val grid = job.get()
    if (grid.getModels.isEmpty) {
      throw new IllegalArgumentException("No Model returned.")
    }
    val unsortedGridModels = grid.getModels.map(m => H2OModel.fromBinary(m, getHyperParameters().asScala.keys.toArray))
    gridModels = sortGridModels(unsortedGridModels)
    gridMojoModels = gridModels.map { m =>
      val data = ModelSerializationSupport.getMojoData(DKV.getGet(m.modelId))
      H2OMOJOModel.createFromMojo(data, Identifiable.randomUID(s"${algoParams.algoName()}_mojoModel"))
    }
    val firstModel = extractFirstModelFromGrid()
    ModelSerializationSupport.getMojoData(DKV.getGet[H2OBaseModel](firstModel.modelId))
  }

  private def fitOverRest(algoParams: Model.Parameters,
                          hyperParams: util.HashMap[String, Array[AnyRef]],
                          trainKey: String,
                          validKey: Option[String]): Array[Byte] = {
    // TODO: implement over REST
    algoParams._train = DKV.getGet[Frame](trainKey)._key
    algoParams._valid = validKey.map(DKV.getGet[Frame](_)._key).orNull

    algoParams._nfolds = getNfolds()
    algoParams._fold_column = getFoldCol()
    algoParams._response_column = getLabelCol()
    algoParams._weights_column = getWeightCol()
    val trainFrame = algoParams._train.get()

    water.DKV.put(trainFrame)
    val Cartesian = HyperSpaceSearchCriteria.Strategy.Cartesian.name()
    val RandomDiscrete = HyperSpaceSearchCriteria.Strategy.RandomDiscrete.name()
    val criteria = getStrategy() match {
      case Cartesian => new CartesianSearchCriteria
      case RandomDiscrete =>
        val c = new RandomDiscreteValueSearchCriteria
        c.set_stopping_tolerance(getStoppingTolerance())
        c.set_stopping_rounds(getStoppingRounds())
        c.set_stopping_metric(ScoreKeeper.StoppingMetric.valueOf(getStoppingMetric()))
        c.set_seed(getSeed())
        c.set_max_models(getMaxModels())
        c.set_max_runtime_secs(getMaxRuntimeSecs())
        c
      case _ => new CartesianSearchCriteria
    }

    val paramsBuilder = algoParams match {
      case _: GBMParameters => new SimpleParametersBuilderFactory[GBMParameters]
      case _: DeepLearningParameters => new SimpleParametersBuilderFactory[DeepLearningParameters]
      case _: GLMParameters => new SimpleParametersBuilderFactory[GLMParameters]
      case _: XGBoostParameters => new SimpleParametersBuilderFactory[XGBoostParameters]
      case _: DRFParameters => new SimpleParametersBuilderFactory[DRFParameters]
      case algo => throw new IllegalArgumentException("Unsupported Algorithm " + algo.algoName())
    }
    val job = GridSearch.startGridSearch(Key.make(), algoParams, hyperParams,
      paramsBuilder.asInstanceOf[SimpleParametersBuilderFactory[Model.Parameters]], criteria, GridSearch.getParallelismLevel(getParallelism()))
    // Block until GridSearch finishes
    val grid = job.get()
    if (grid.getModels.isEmpty) {
      throw new IllegalArgumentException("No Model returned.")
    }
    val unsortedGridModels = grid.getModels.map(m => H2OModel.fromBinary(m, getHyperParameters().asScala.keys.toArray))
    gridModels = sortGridModels(unsortedGridModels)
    gridMojoModels = gridModels.map { m =>
      val data = ModelSerializationSupport.getMojoData(DKV.getGet(m.modelId))
      H2OMOJOModel.createFromMojo(data, Identifiable.randomUID(s"${algoParams.algoName()}_mojoModel"))
    }
    val firstModel = extractFirstModelFromGrid()
    ModelSerializationSupport.getMojoData(DKV.getGet[H2OBaseModel](firstModel.modelId))
  }

  override def fit(dataset: Dataset[_]): H2OMOJOModel = {
    val algoParams = extractH2OParameters(getAlgo())

    if (algoParams == null) {
      throw new IllegalArgumentException(s"Algorithm has to be specified. Available algorithms are " +
        s"${H2OGridSearch.SupportedAlgos.values.mkString(", ")}")
    }

    val hyperParams = processHyperParams(algoParams, getHyperParameters())
    val (trainKey, validKey, internalFeatureCols) = prepareDatasetForFitting(dataset)
    val mojoData = if (RestApiUtils.isRestAPIBased()) {
      fitOverRest(algoParams, hyperParams, trainKey, validKey)
    } else {
      fitOverClient(algoParams, hyperParams, trainKey, validKey)
    }
    val modelSettings = H2OMOJOSettings.createFromModelParams(this)
    H2OMOJOModel.createFromMojo(
      mojoData,
      Identifiable.randomUID(ModelSerializationSupport.getMojoModel(mojoData)._algoName),
      modelSettings,
      internalFeatureCols)
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

  private def getGridModels(gridKey: String): Array[H2OModel] = {
    if (RestApiUtils.isRestAPIBased()) {
      throw new UnsupportedOperationException
    } else {
      val grid = DKV.getGet[Grid[_ <: Model.Parameters]](gridKey)
      if (grid.getModels.isEmpty) {
        throw new IllegalArgumentException("No Model returned.")
      }
      grid.getModels.map(m => H2OModel.fromBinary(m, getHyperParameters().asScala.keys.toArray))
    }
  }

  private def sortGridModels(gridModels: Array[H2OModel]): Array[H2OModel] = {
    val metric = if (getSelectBestModelBy() == H2OMetric.AUTO.name()) {
      gridModels(0).modelCategory match {
        case H2OModelCategory.Regression => H2OMetric.RMSE
        case H2OModelCategory.Binomial => H2OMetric.AUC
        case H2OModelCategory.Multinomial => H2OMetric.Logloss
      }
    } else {
      H2OMetric.valueOf(getSelectBestModelBy())
    }

    val modelMetricPair = gridModels.map { model =>
      (model, model.getCurrentMetrics(getNfolds(), getSplitRatio()).find(_._1 == metric).get._2)
    }

    val ordering = if (metric.higherTheBetter) Ordering.Double.reverse else Ordering.Double
    modelMetricPair.sortBy(_._2)(ordering).map(_._1)
  }

  private def extractFirstModelFromGrid(): H2OModel = {
    if (gridModels.isEmpty) {
      throw new IllegalArgumentException("No model returned.")
    }
    gridModels.head
  }

  private def ensureGridSearchIsFitted() : Unit = {
    require(gridMojoModels != null, "The fit method of the grid search must be called first to be able to obtain a list of models.")
  }

  def getGridModelsParams(): DataFrame = {
    ensureGridSearchIsFitted()
    val hyperParamNames = getHyperParameters().keySet().asScala.toSeq
    val rowValues = gridModels.zip(gridMojoModels.map(_.uid)).map { case (model, id) =>
      val outputParams = model.trainingParams.filter { case (key, _) => hyperParamNames.contains(key) }
      Row(Seq(id) ++ outputParams.values: _*)
    }

    val colNames = gridModels.headOption.map { model =>
      val outputParams = model.trainingParams.filter { case (key, _) => hyperParamNames.contains(key) }
      outputParams.keys.map(StructField(_, StringType, nullable = false)).toList
    }.getOrElse(List.empty)


    val schema = StructType(List(StructField("Mojo Model ID", StringType, nullable = false)) ++ colNames)
    val spark = SparkSessionUtils.active
    spark.createDataFrame(spark.sparkContext.parallelize(rowValues), schema)
  }

  def getGridModelsMetrics(): DataFrame = {
    ensureGridSearchIsFitted()
    val rowValues = gridModels.zip(gridMojoModels.map(_.uid)).map { case (model, id) =>
      val metrics = model.getCurrentMetrics(getNfolds(), getSplitRatio())
      Row(Seq(id) ++ metrics.values: _*)
    }
    val colNames = gridModels.headOption.map { model =>
      val metrics = model.getCurrentMetrics(getNfolds(), getSplitRatio())
      metrics.map(_._1.toString).map(StructField(_, DoubleType, nullable = false)).toList
    }.getOrElse(List.empty)

    val schema = StructType(List(StructField("Model ID", StringType, nullable = false)) ++ colNames)
    val spark = SparkSessionUtils.active
    spark.createDataFrame(spark.sparkContext.parallelize(rowValues), schema)
  }

  def getGridModels(): Array[H2OMOJOModel] = {
    ensureGridSearchIsFitted()
    gridMojoModels
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  private def extractH2OParameters(algo: H2OSupervisedAlgorithm[_, _, _ <: Model.Parameters]): Model.Parameters = {
    val m = algo.getClass.getDeclaredMethod("updateH2OParams")
    m.setAccessible(true)
    m.invoke(algo)

    val field = PojoUtils.getFieldEvenInherited(algo, "parameters")
    field.setAccessible(true)
    field.get(algo).asInstanceOf[Model.Parameters]
  }
}

object H2OGridSearch extends H2OParamsReadable[H2OGridSearch] {

  object SupportedAlgos extends Enumeration {
    val H2OGBM, H2OGLM, H2ODeepLearning, H2OXGBoost, H2ODRF = Value

    def checkIfSupported(algo: H2OAlgorithm[_, _, _ <: Model.Parameters]): Unit = {
      val exists = values.exists(_.toString == algo.getClass.getSimpleName)
      if (!exists) {
        throw new IllegalArgumentException(s"Grid Search is not supported for the specified algorithm '${algo.getClass}'. Supported " +
          s"algorithms are ${H2OGridSearch.SupportedAlgos.values.mkString(", ")}")
      }
    }
  }
}

trait H2OGridSearchParams extends H2OCommonSupervisedParams {

  //
  // Param definitions
  //
  private val algo = new AlgoParam(this, "algo", "Specifies the algorithm for grid search")
  private val hyperParameters = new HyperParamsParam(this, "hyperParameters", "Hyper Parameters")
  private val strategy = new Param[String](this, "strategy", "Search criteria strategy")
  private val maxRuntimeSecs = new DoubleParam(this, "maxRuntimeSecs", "maxRuntimeSecs")
  private val maxModels = new IntParam(this, "maxModels", "maxModels")
  private val stoppingRounds = new IntParam(this, "stoppingRounds", "Early stopping based on convergence of stoppingMetric")
  private val stoppingTolerance = new DoubleParam(this, "stoppingTolerance", "Relative tolerance for metric-based" +
    " stopping criterion: stop if relative improvement is not at least this much.")
  private val stoppingMetric = new Param[String](this, "stoppingMetric", "Stopping Metric")
  private val selectBestModelBy = new Param[String](this, "selectBestModelBy", "Select best model by specific metric." +
    "If this value is not specified that the first model os taken.")
  private val parallelism = new IntParam(this,
    "parallelism",
    """Level of model-building parallelism, the possible values are:
      | 0 -> H2O selects parallelism level based on cluster configuration, such as number of cores
      | 1 -> Sequential model building, no parallelism
      | n>1 -> n models will be built in parallel if possible""".stripMargin)
  //
  // Default values
  //
  setDefault(
    algo -> null,
    hyperParameters -> Map.empty[String, Array[AnyRef]].asJava,
    strategy -> HyperSpaceSearchCriteria.Strategy.Cartesian.name(),
    maxRuntimeSecs -> 0,
    maxModels -> 0,
    stoppingRounds -> 0,
    stoppingTolerance -> 0.001,
    stoppingMetric -> ScoreKeeper.StoppingMetric.AUTO.name(),
    selectBestModelBy -> H2OMetric.AUTO.name(),
    parallelism -> 1
  )

  //
  // Getters
  //
  def getAlgo(): H2OSupervisedAlgorithm[_, _, _ <: Model.Parameters] = $(algo)

  def getHyperParameters(): util.Map[String, Array[AnyRef]] = $(hyperParameters)

  def getStrategy(): String = $(strategy)

  def getMaxRuntimeSecs(): Double = $(maxRuntimeSecs)

  def getMaxModels(): Int = $(maxModels)

  def getStoppingRounds(): Int = $(stoppingRounds)

  def getStoppingTolerance(): Double = $(stoppingTolerance)

  def getStoppingMetric(): String = $(stoppingMetric)

  def getSelectBestModelBy(): String = $(selectBestModelBy)

  def getParallelism(): Int = $(parallelism)

  //
  // Setters
  //
  def setAlgo(value: H2OSupervisedAlgorithm[_, _, _ <: Model.Parameters]): this.type = {
    H2OGridSearch.SupportedAlgos.checkIfSupported(value)
    set(algo, value)
  }

  def setHyperParameters(value: Map[String, Array[AnyRef]]): this.type = set(hyperParameters, value.asJava)

  def setHyperParameters(value: mutable.Map[String, Array[AnyRef]]): this.type = set(hyperParameters, value.toMap.asJava)

  def setHyperParameters(value: java.util.Map[String, Array[AnyRef]]): this.type = set(hyperParameters, value)

  def setStrategy(value: String): this.type = {
    val validated = H2OAlgoParamsHelper.getValidatedEnumValue[HyperSpaceSearchCriteria.Strategy](value)
    set(strategy, validated)
  }

  def setMaxRuntimeSecs(value: Double): this.type = set(maxRuntimeSecs, value)

  def setMaxModels(value: Int): this.type = set(maxModels, value)

  def setStoppingRounds(value: Int): this.type = set(stoppingRounds, value)

  def setStoppingTolerance(value: Double): this.type = set(stoppingTolerance, value)

  def setStoppingMetric(value: String): this.type = {
    val validated = H2OAlgoParamsHelper.getValidatedEnumValue[ScoreKeeper.StoppingMetric](value)
    set(stoppingMetric, validated)
  }

  def setSelectBestModelBy(value: String): this.type = {
    val validated = H2OAlgoParamsHelper.getValidatedEnumValue[H2OMetric](value)
    set(selectBestModelBy, validated)
  }

  def setParallelism(value: Int): this.type = set(parallelism, value)
}
