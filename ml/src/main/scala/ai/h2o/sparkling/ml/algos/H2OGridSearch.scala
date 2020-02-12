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

import ai.h2o.sparkling.ml.models.{H2OMOJOModel, H2OMOJOSettings}
import ai.h2o.sparkling.ml.params.{AlgoParam, H2OAlgoParamsHelper, H2OCommonSupervisedParams, HyperParamsParam}
import ai.h2o.sparkling.ml.utils.H2OParamsReadable
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.glm.GLMModel.GLMParameters
import hex.grid.GridSearch.SimpleParametersBuilderFactory
import hex.grid.HyperSpaceSearchCriteria.{CartesianSearchCriteria, RandomDiscreteValueSearchCriteria}
import hex.grid.{Grid, GridSearch, HyperSpaceSearchCriteria}
import hex.tree.drf.DRFModel.DRFParameters
import hex.tree.gbm.GBMModel.GBMParameters
import hex.tree.xgboost.XGBoostModel.XGBoostParameters
import hex.{Model, ModelMetricsBinomial, ModelMetricsBinomialGLM, ModelMetricsMultinomial, ModelMetricsRegression, ModelMetricsRegressionGLM, ScoreKeeper}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.h2o._
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import water.{DKV, Key}
import water.support.{H2OFrameSupport, ModelSerializationSupport}
import water.util.PojoUtils

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * H2O Grid Search algorithm exposed via Spark ML pipelines.
  *
  */
class H2OGridSearch(override val uid: String) extends Estimator[H2OMOJOModel]
  with H2OAlgoCommonUtils with DefaultParamsWritable with H2OGridSearchParams {

  private lazy val hc = H2OContext.getOrCreate(SparkSession.builder().getOrCreate())

  def this() = this(Identifiable.randomUID(classOf[H2OGridSearch].getSimpleName))

  private var grid: Grid[_ <: Model.Parameters] = _
  private var gridModels: Array[H2OBaseModel] = _
  private var gridMojoModels: Array[H2OMOJOModel] = _

  override def fit(dataset: Dataset[_]): H2OMOJOModel = {
    val algoParams = extractH2OParameters(getAlgo())

    if (algoParams == null) {
      throw new IllegalArgumentException(s"Algorithm has to be specified. Available algorithms are " +
        s"${H2OGridSearch.SupportedAlgos.values.mkString(", ")}")
    }

    val hyperParams = processHyperParams(algoParams, getHyperParameters())

    val (trainKey, validKey, internalFeatureCols) = prepareDatasetForFitting(dataset)
    algoParams._train = DKV.getGet[Frame](trainKey)._key
    algoParams._valid = validKey.map(DKV.getGet[Frame](_)._key).orNull

    algoParams._nfolds = getNfolds()
    algoParams._fold_column = getFoldCol()
    algoParams._response_column = getLabelCol()
    algoParams._weights_column = getWeightCol()
    val trainFrame = algoParams._train.get()
    if (getAllStringColumnsToCategorical()) {
      H2OFrameSupport.allStringVecToCategorical(trainFrame)
    }
    H2OFrameSupport.columnsToCategorical(trainFrame, getColumnsToCategorical())

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
    grid = job.get()
    gridModels = sortGrid(grid)
    gridMojoModels = gridModels.map { m =>
      val data = ModelSerializationSupport.getMojoData(m)
      H2OMOJOModel.createFromMojo(data, Identifiable.randomUID(s"${algoParams.algoName()}_mojoModel"))
    }

    val binaryModel = selectModelFromGrid(grid)
    val mojoData = ModelSerializationSupport.getMojoData(binaryModel)
    val modelSettings = H2OMOJOSettings.createFromModelParams(this)
    H2OMOJOModel.createFromMojo(
      mojoData,
      Identifiable.randomUID(binaryModel._parms.algoName()),
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

  private def selectMetric(model: H2OBaseModel) = {
    if (getNfolds() > 1) {
      // use cross validation metrics
      model._output._cross_validation_metrics
    } else if (getSplitRatio() < 1) {
      // some portion of data is reserved for validation, use validation metrics
      model._output._validation_metrics
    } else {
      // use training metrics
      model._output._training_metrics
    }
  }

  private def sortGrid(grid: Grid[_ <: Model.Parameters]): Array[H2OBaseModel] = {
    if (grid.getModels.isEmpty) {
      throw new IllegalArgumentException("No Model returned.")
    }

    val metric = if (getSelectBestModelBy() == H2OGridSearchMetric.AUTO.name()) {
      selectMetric(grid.getModels()(0)) match {
        case _: ModelMetricsRegression => H2OGridSearchMetric.RMSE
        case _: ModelMetricsBinomial => H2OGridSearchMetric.AUC
        case _: ModelMetricsMultinomial => H2OGridSearchMetric.Logloss
      }
    } else {
      H2OGridSearchMetric.valueOf(getSelectBestModelBy())
    }

    val modelMetricPair = grid.getModels.map { m =>
      (m, extractMetrics(m).find(_._1 == metric).get._2)
    }

    val ordering = if (metric.higherTheBetter) Ordering.Double.reverse else Ordering.Double
    modelMetricPair.sortBy(_._2)(ordering).map(_._1)
  }

  def selectModelFromGrid(grid: Grid[_]): H2OBaseModel = {
    if (gridModels.isEmpty) {
      throw new IllegalArgumentException("No Model returned.")
    }
    grid.getModels.head
  }


  private def extractMetrics(model: H2OBaseModel) = {
    // Supervised metrics
    val mm = selectMetric(model)
    val metricPairs = mm match {
      case regressionGLM: ModelMetricsRegressionGLM =>
        Seq(
          (H2OGridSearchMetric.MeanResidualDeviance, regressionGLM._mean_residual_deviance),
          (H2OGridSearchMetric.NullDeviance, regressionGLM._resDev),
          (H2OGridSearchMetric.ResidualDegreesOfFreedom, regressionGLM._residualDegressOfFreedom.toDouble),
          (H2OGridSearchMetric.NullDeviance, regressionGLM._nullDev),
          (H2OGridSearchMetric.NullDegreesOfFreedom, regressionGLM._nullDegressOfFreedom.toDouble),
          (H2OGridSearchMetric.AIC, regressionGLM._AIC),
          (H2OGridSearchMetric.R2, regressionGLM.r2())
        )
      case regression: ModelMetricsRegression =>
        Seq(
          (H2OGridSearchMetric.MeanResidualDeviance, regression._mean_residual_deviance),
          (H2OGridSearchMetric.R2, regression.r2())
        )
      case binomialGLM: ModelMetricsBinomialGLM =>
        Seq(
          (H2OGridSearchMetric.AUC, binomialGLM.auc),
          (H2OGridSearchMetric.Gini, binomialGLM._auc._gini),
          (H2OGridSearchMetric.Logloss, binomialGLM.logloss),
          (H2OGridSearchMetric.F1, binomialGLM.cm.f1),
          (H2OGridSearchMetric.F2, binomialGLM.cm.f2),
          (H2OGridSearchMetric.F0point5, binomialGLM.cm.f0point5),
          (H2OGridSearchMetric.Accuracy, binomialGLM.cm.accuracy),
          (H2OGridSearchMetric.Error, binomialGLM.cm.err),
          (H2OGridSearchMetric.Precision, binomialGLM.cm.precision),
          (H2OGridSearchMetric.Recall, binomialGLM.cm.recall),
          (H2OGridSearchMetric.MCC, binomialGLM.cm.mcc),
          (H2OGridSearchMetric.MaxPerClassError, binomialGLM.cm.max_per_class_error),
          (H2OGridSearchMetric.ResidualDeviance, binomialGLM._resDev),
          (H2OGridSearchMetric.ResidualDegreesOfFreedom, binomialGLM._residualDegressOfFreedom.toDouble),
          (H2OGridSearchMetric.NullDeviance, binomialGLM._nullDev),
          (H2OGridSearchMetric.NullDegreesOfFreedom, binomialGLM._nullDegressOfFreedom.toDouble),
          (H2OGridSearchMetric.AIC, binomialGLM._AIC)
        )
      case binomial: ModelMetricsBinomial =>
        Seq(
          (H2OGridSearchMetric.AUC, binomial.auc),
          (H2OGridSearchMetric.Gini, binomial._auc._gini),
          (H2OGridSearchMetric.Logloss, binomial.logloss),
          (H2OGridSearchMetric.F1, binomial.cm.f1),
          (H2OGridSearchMetric.F2, binomial.cm.f2),
          (H2OGridSearchMetric.F0point5, binomial.cm.f0point5),
          (H2OGridSearchMetric.Accuracy, binomial.cm.accuracy),
          (H2OGridSearchMetric.Error, binomial.cm.err),
          (H2OGridSearchMetric.Precision, binomial.cm.precision),
          (H2OGridSearchMetric.Recall, binomial.cm.recall),
          (H2OGridSearchMetric.MCC, binomial.cm.mcc),
          (H2OGridSearchMetric.MaxPerClassError, binomial.cm.max_per_class_error)
        )

      case multinomial: ModelMetricsMultinomial =>
        Seq(
          (H2OGridSearchMetric.Logloss, multinomial.logloss),
          (H2OGridSearchMetric.Error, multinomial.cm.err),
          (H2OGridSearchMetric.MaxPerClassError, multinomial.cm.max_per_class_error),
          (H2OGridSearchMetric.Accuracy, multinomial.cm.accuracy)
        )
      case _ => Seq()
    }

    Seq(
      (H2OGridSearchMetric.MSE, mm.mse),
      (H2OGridSearchMetric.RMSE, mm.rmse())
    ) ++ metricPairs
  }

  private def ensureGridSearchIsFitted() : Unit = {
    require(gridMojoModels != null, "The fit method of the grid search must be called first to be able to obtain a list of models.")
  }

  def getGridModelsParams() = {
    ensureGridSearchIsFitted()

    val hyperParamNames = getHyperParameters().keySet().asScala.toSeq
    val rows = gridModels.zip(gridMojoModels.map(_.uid)).map { case (m, id) =>

    def fieldValueToString(value: Any): String = value match {
      case a: Array[_] => a.map(fieldValueToString).mkString("[", ", ", "]")
      case v => v.toString
    }

      val paramValues = Seq(id) ++ hyperParamNames.map { param =>
        val value = PojoUtils.getFieldEvenInherited(m._parms, param).get(m._parms)
        fieldValueToString(value)
      }

      Row(paramValues: _*)
    }

    val paramNames = hyperParamNames.map(StructField(_, StringType, nullable = false)).toList
    val schema = StructType(List(StructField("Mojo Model ID", StringType, nullable = false)) ++ paramNames)
    val fr = hc.sparkSession.createDataFrame(hc.sparkContext.parallelize(rows), schema)
    fr
  }

  def getGridModelsMetrics() = {
    ensureGridSearchIsFitted()
    require(gridMojoModels.nonEmpty, "No model returned.")

    val rows = gridModels.zip(gridMojoModels.map(_.uid)).map { case (m, id) =>
      val metrics = extractMetrics(m)
      val metricValues = Seq(id) ++ metrics.map(_._2)

      val values = metricValues
      Row(values: _*)
    }

    val metricNames = extractMetrics(grid.getModels()(0)).
      map(_._1.toString).map(StructField(_, DoubleType, nullable = false)).toList
    val schema = StructType(List(StructField("Model ID", StringType, nullable = false)) ++ metricNames)
    val fr = hc.sparkSession.createDataFrame(hc.sparkContext.parallelize(rows), schema)
    fr
  }

  def getGridModels() = {
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
    selectBestModelBy -> H2OGridSearchMetric.AUTO.name(),
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
    val validated = H2OAlgoParamsHelper.getValidatedEnumValue[H2OGridSearchMetric](value)
    set(selectBestModelBy, validated)
  }

  def setParallelism(value: Int): this.type = set(parallelism, value)
}
