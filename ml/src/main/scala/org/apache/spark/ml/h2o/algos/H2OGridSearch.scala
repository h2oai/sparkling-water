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

import java.lang.reflect.Field
import java.util

import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.glm.GLMModel.GLMParameters
import hex.grid.GridSearch.SimpleParametersBuilderFactory
import hex.grid.HyperSpaceSearchCriteria.{CartesianSearchCriteria, RandomDiscreteValueSearchCriteria}
import hex.grid.{Grid, GridSearch, HyperSpaceSearchCriteria}
import hex.tree.gbm.GBMModel.GBMParameters
import hex.tree.xgboost.XGBoostModel.XGBoostParameters
import hex.{Model, ModelMetricsBinomial, ModelMetricsBinomialGLM, ModelMetricsMultinomial, ModelMetricsRegression, ModelMetricsRegressionGLM, ScoreKeeper}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.h2o._
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.h2o.models.H2OMOJOModel
import org.apache.spark.ml.h2o.param._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import water.Key
import water.support.{H2OFrameSupport, ModelSerializationSupport}
import water.util.{DeprecatedMethod, PojoUtils}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * H2O Grid Search
  */
class H2OGridSearch(override val uid: String) extends Estimator[H2OMOJOModel]
  with DefaultParamsWritable with H2OGridSearchParams {

  private lazy val hc = H2OContext.getOrCreate(SparkSession.builder().getOrCreate())

  def this() = this(Identifiable.randomUID("gridsearch"))

  private var grid: Grid[_ <: Model.Parameters] = _
  private var gridModels: Array[H2OBaseModel] = _
  private var gridMojoModels: Array[H2OMOJOModel] = _

  override def fit(dataset: Dataset[_]): H2OMOJOModel = {
    val algoParams = ${gridAlgoParams}

    if (algoParams == null) {
      throw new IllegalArgumentException(s"Algorithm has to be specified. Available algorithms are " +
        s"${H2OGridSearch.SupportedAlgos.allAsString}")
    }

    if (!H2OGridSearch.SupportedAlgos.isSupportedAlgo(algoParams.algoName())) {
      throw new IllegalArgumentException(s"Grid Search is not supported for the specified algorithm '${algoParams.algoName()}'. Supported " +
        s"algorithms are ${H2OGridSearch.SupportedAlgos.allAsString}")
    }

    val hyperParams = processHyperParams(algoParams, getHyperParameters())

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
    algoParams._nfolds = getNfolds()
    algoParams._fold_column = getFoldCol()
    algoParams._response_column = getLabelCol()
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
        c.set_stopping_tolerance(getStoppingTolerance())
        c.set_stopping_rounds(getStoppingRounds())
        c.set_stopping_metric(getStoppingMetric())
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
      case algo => throw new IllegalArgumentException("Unsupported Algorithm " + algo.algoName())
    }
    val job = GridSearch.startGridSearch(Key.make(), algoParams, hyperParams,
      paramsBuilder.asInstanceOf[SimpleParametersBuilderFactory[Model.Parameters]], criteria)
    grid = job.get()
    gridModels = sortGrid(grid)
    gridMojoModels = gridModels.map { m =>
      val data = ModelSerializationSupport.getMojoData(m)
      new H2OMOJOModel(data, Identifiable.randomUID(s"${$(gridAlgoParams).algoName()}_mojoModel"))
    }

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
    new H2OMOJOModel(ModelSerializationSupport.getMojoData(selectModelFromGrid(grid)), Identifiable.randomUID("gridSearch_mojoModel"))
  }

  private def selectMetric(model: H2OBaseModel) = {
    if (getNfolds() > 1) {
      // use cross validation metrics
      model._output._cross_validation_metrics
    } else if (getRatio() < 1) {
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

    val metric = if (getSelectBestModelBy() == null) {
      selectMetric(grid.getModels()(0)) match {
        case _: ModelMetricsRegression => H2OGridSearchMetric.RMSE
        case _: ModelMetricsBinomial => H2OGridSearchMetric.AUC
        case _: ModelMetricsMultinomial => H2OGridSearchMetric.Logloss
      }
    } else {
      getSelectBestModelBy()
    }

    val modelMetricPair = grid.getModels.map { m =>
      (m, extractMetrics(m).find(_._1 == metric).get._2)
    }

    val ordering = if (getSelectBestModelBy() == null) {
      logWarning("You did not specify 'selectBestModelBy' parameter, but specified 'selectBestModelDecreasing'." +
        " In the case 'selectBestModelBy' is not specified, we sort the grid models by default metric and ignore the ordering" +
        " specified by 'selectBestModelDecreasing'." +
        " If you still wish to use the specific ordering, please make sure to explicitly select the metric which you want to" +
        " order.")
      // in case the user did not specified the metric, override the ordering to ensure we return the best model first
      grid.getModels()(0)._output._training_metrics match {
        case _: ModelMetricsRegression => Ordering.Double
        case _: ModelMetricsBinomial => Ordering.Double.reverse
        case _: ModelMetricsMultinomial => Ordering.Double
      }
    } else {
      if (getSelectBestModelDecreasing()) {
        Ordering.Double.reverse
      } else {
        Ordering.Double
      }
    }


    modelMetricPair.sortBy(_._2)(ordering).map(_._1)
  }

  def selectModelFromGrid(grid: Grid[_]) = {
    if (gridModels.isEmpty) {
      throw new IllegalArgumentException("No Model returned.")
    }
    grid.getModels()(0)
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


  def getGridModelsParams() = {
    val hyperParamNames = getHyperParameters().keySet().asScala.toSeq
    val rows = gridModels.zip(gridMojoModels.map(_.uid)).map { case (m, id) =>

      val paramValues = Seq(id) ++ hyperParamNames.map { param =>
        PojoUtils.getFieldEvenInherited(m._parms, param).get(m._parms).toString
      }

      Row(paramValues: _*)
    }

    val paramNames = hyperParamNames.map(StructField(_, StringType, nullable = false)).toList
    val schema = StructType(List(StructField("Mojo Model ID", StringType, nullable = false)) ++ paramNames)
    val fr = hc.sparkSession.createDataFrame(hc.sparkContext.parallelize(rows), schema)
    fr
  }

  def getGridModelsMetrics() = {
    if (grid == null) {
      throw new IllegalArgumentException("The model must be first fit to be able to obtain list of grid search algorithms")
    }
    if (gridModels.isEmpty) {
      throw new IllegalArgumentException("No model returned.")
    }

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
    if (gridMojoModels == null) {
      throw new IllegalArgumentException("The model must be first fit to be able to obtain list of grid search algorithms")
    }
    gridMojoModels
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}

object H2OGridSearch extends DefaultParamsReadable[py_sparkling.ml.algos.H2OGridSearch] {
  object SupportedAlgos extends Enumeration {
    val gbm, glm, deeplearning = Value // still missing pipeline wrappers for KMeans & drf

    def isSupportedAlgo(s: String) = values.exists(_.toString == s.toLowerCase())

    def allAsString = values.mkString(", ")

    def fromString(value: String) = values.find(_.toString == value.toLowerCase())
  }

  object MetricOrder extends Enumeration {
    type MetricOrder = Value
    val Asc, Desc = Value
  }
}

trait H2OGridSearchParams extends DeprecatableParams {

  override protected def renamingMap: Map[String, String] = Map(
    "predictionCol" -> "labelCol"
  )

  //
  // Param definitions
  //
  private val algo = new DoubleParam(this, "algo", "dummy argument for pysparkling")
  private val ratio = new DoubleParam(this, "ratio", "Determines in which ratios split the dataset")
  protected final val gridAlgoParams = new AlgoParams(this, "algoParams", "Specifies the algorithm for grid search")
  private val hyperParameters = new HyperParamsParam(this, "hyperParameters", "Hyper Parameters")
  private val labelCol = new Param[String](this, "labelCol", "Label column name")
  private val allStringColumnsToCategorical = new BooleanParam(this, "allStringColumnsToCategorical", "Transform all strings columns to categorical")
  private val columnsToCategorical = new StringArrayParam(this, "columnsToCategorical", "List of columns to convert to categoricals before modelling")
  private val strategy = new GridSearchStrategyParam(this, "strategy", "Search criteria strategy")
  private val maxRuntimeSecs = new DoubleParam(this, "maxRuntimeSecs", "maxRuntimeSecs")
  private val maxModels = new IntParam(this, "maxModels", "maxModels")
  private val seed = new LongParam(this, "seed", "seed for hyper params search")
  private val stoppingRounds = new IntParam(this, "stoppingRounds", "Early stopping based on convergence of stoppingMetric")
  private val stoppingTolerance = new DoubleParam(this, "stoppingTolerance", "Relative tolerance for metric-based" +
    " stopping criterion: stop if relative improvement is not at least this much.")
  private val stoppingMetric = new StoppingMetricParam(this, "stoppingMetric", "Stopping Metric")
  private val nfolds = new IntParam(this, "nfolds", "nfolds")
  private val selectBestModelBy = new MetricParam(this, "selectBestModelBy", "Select best model by specific metric." +
    "If this value is not specified that the first model os taken.")
  private val selectBestModelDecreasing = new BooleanParam(this, "selectBestModelDecreasing",
    "True if sort in decreasing order accordingto selected metrics")
  private val foldCol = new NullableStringParam(this, "foldCol", "Fold column name")

  //
  // Default values
  //
  setDefault(
    gridAlgoParams -> null,
    ratio -> 1.0, // 1.0 means use whole frame as training frame
    hyperParameters -> Map.empty[String, Array[AnyRef]].asJava,
    labelCol -> "label",
    allStringColumnsToCategorical -> true,
    columnsToCategorical -> Array.empty[String],
    strategy -> HyperSpaceSearchCriteria.Strategy.Cartesian,
    maxRuntimeSecs -> 0,
    maxModels -> 0,
    seed -> -1,
    stoppingRounds -> 0,
    stoppingTolerance -> 0.001,
    stoppingMetric -> ScoreKeeper.StoppingMetric.AUTO,
    nfolds -> 0,
    selectBestModelBy -> null,
    selectBestModelDecreasing -> true,
    foldCol -> null
  )

  //
  // Getters
  //
  def getRatio(): Double = $(ratio)

  def getHyperParameters(): util.Map[String, Array[AnyRef]] = $(hyperParameters)

  @DeprecatedMethod("getLabelCol")
  def getPredictionCol(): String = getLabelCol()

  def getLabelCol(): String = $(labelCol)

  def getAllStringColumnsToCategorical(): Boolean = $(allStringColumnsToCategorical)

  def getColumnsToCategorical(): Array[String] = $(columnsToCategorical)

  def getStrategy(): HyperSpaceSearchCriteria.Strategy = $(strategy)

  def getMaxRuntimeSecs(): Double = $(maxRuntimeSecs)

  def getMaxModels(): Int = $(maxModels)

  def getSeed(): Long = $(seed)

  def getStoppingRounds(): Int = $(stoppingRounds)

  def getStoppingTolerance(): Double = $(stoppingTolerance)

  def getStoppingMetric(): ScoreKeeper.StoppingMetric = $(stoppingMetric)

  def getNfolds(): Int = $(nfolds)

  def getSelectBestModelBy(): H2OGridSearchMetric = $(selectBestModelBy)

  def getSelectBestModelDecreasing(): Boolean = $(selectBestModelDecreasing)

  def getFoldCol(): String = $(foldCol)

  //
  // Setters
  //
  def setRatio(value: Double): this.type = set(ratio, value)

  def setAlgo(value: H2OAlgorithm[_ <: Model.Parameters]): this.type = {
    val field = PojoUtils.getFieldEvenInherited(value, "parameters")
    field.setAccessible(true)
    val algoParams = field.get(value).asInstanceOf[Model.Parameters]
    if (!H2OGridSearch.SupportedAlgos.isSupportedAlgo(algoParams.algoName())) {
      throw new IllegalArgumentException(s"Grid Search is not supported for the specified algorithm '$value'. Supported " +
        s"algorithms are ${H2OGridSearch.SupportedAlgos.allAsString}")
    }
    set(gridAlgoParams, algoParams)
  }

  def setHyperParameters(value: Map[String, Array[AnyRef]]): this.type = set(hyperParameters, value.asJava)

  def setHyperParameters(value: mutable.Map[String, Array[AnyRef]]): this.type = set(hyperParameters, value.toMap.asJava)

  def setHyperParameters(value: java.util.Map[String, Array[AnyRef]]): this.type = set(hyperParameters, value)

  @DeprecatedMethod("setLabelCol")
  def setPredictionCol(value: String): this.type = setLabelCol(value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setAllStringColumnsToCategorical(value: Boolean): this.type = set(allStringColumnsToCategorical, value)

  def setColumnsToCategorical(first: String, others: String*): this.type = set(columnsToCategorical, Array(first) ++ others)

  def setColumnsToCategorical(columns: Array[String]): this.type = set(columnsToCategorical, columns)

  def setStrategy(value: HyperSpaceSearchCriteria.Strategy): this.type = set(strategy, value)

  def setMaxRuntimeSecs(value: Double): this.type = set(maxRuntimeSecs, value)

  def setMaxModels(value: Int): this.type = set(maxModels, value)

  def setSeed(value: Long): this.type = set(seed, value)

  def setStoppingRounds(value: Int): this.type = set(stoppingRounds, value)

  def setStoppingTolerance(value: Double): this.type = set(stoppingTolerance, value)

  def setStoppingMetric(value: ScoreKeeper.StoppingMetric): this.type = set(stoppingMetric, value)

  def setNfolds(value: Int): this.type = set(nfolds, value)

  def setSelectBestModelBy(value: H2OGridSearchMetric): this.type = set(selectBestModelBy, value)

  def setSelectBestModelDecreasing(value: Boolean): this.type = set(selectBestModelDecreasing, value)

  def setFoldCol(value: String): this.type = set(foldCol, value)

}

class GridSearchStrategyParam private[h2o](parent: Params, name: String, doc: String,
                                           isValid: HyperSpaceSearchCriteria.Strategy => Boolean)
  extends EnumParam[HyperSpaceSearchCriteria.Strategy](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)
}

class MetricParam private[h2o](parent: Params, name: String, doc: String,
                               isValid: H2OGridSearchMetric => Boolean)
  extends EnumParam[H2OGridSearchMetric](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)
}
