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

import java.util

import ai.h2o.sparkling.backend.external.{RestApiCommunicationException, RestApiUtils, RestCommunication, RestEncodingUtils}
import ai.h2o.sparkling.frame.H2OFrame
import ai.h2o.sparkling.job.H2OJob
import ai.h2o.sparkling.ml.models.{H2OMOJOModel, H2OMOJOSettings}
import ai.h2o.sparkling.ml.params.{AlgoParam, H2OAlgoParamsHelper, H2OCommonSupervisedParams, HyperParamsParam}
import ai.h2o.sparkling.ml.utils.H2OParamsReadable
import ai.h2o.sparkling.model.{H2OMetric, H2OModel, H2OModelCategory}
import ai.h2o.sparkling.utils.ScalaUtils.withResource
import ai.h2o.sparkling.utils.SparkSessionUtils
import com.google.gson.{Gson, JsonElement}
import hex.grid.HyperSpaceSearchCriteria
import hex.{Model, ScoreKeeper}
import org.apache.commons.io.IOUtils
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.h2o.H2OContext
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.JavaConverters._
import scala.collection.mutable

/**
  * H2O Grid Search algorithm exposed via Spark ML pipelines.
  *
  */
class H2OGridSearch(override val uid: String) extends Estimator[H2OMOJOModel]
  with H2OAlgoCommonUtils with DefaultParamsWritable with H2OGridSearchParams
  with RestCommunication
  with RestEncodingUtils {

  def this() = this(Identifiable.randomUID(classOf[H2OGridSearch].getSimpleName))

  private var gridModels: Array[H2OModel] = _
  private var gridMojoModels: Array[H2OMOJOModel] = _

  private def getSearchCriteria(): String = {
    val criteria = HyperSpaceSearchCriteria.Strategy.valueOf(getStrategy()) match {
      case HyperSpaceSearchCriteria.Strategy.RandomDiscrete =>
        Map(
          "strategy" -> HyperSpaceSearchCriteria.Strategy.RandomDiscrete.name(),
          "stopping_tolerance" -> getStoppingTolerance(),
          "stopping_rounds" -> getStoppingRounds(),
          "stopping_metric" -> getStoppingMetric(),
          "seed" -> getSeed(),
          "max_models" -> getMaxModels(),
          "max_runtime_secs" -> getMaxRuntimeSecs()
        )
      case _ => Map("strategy" -> HyperSpaceSearchCriteria.Strategy.Cartesian.name())
    }
    criteria.map { case (key, value) => s"'$key': $value"}.mkString("{", ",", "}")
  }

  private def getAlgoParams(algo: H2OSupervisedAlgorithm[_ <: Model.Parameters],
                            train: H2OFrame,
                            valid: Option[H2OFrame]): Map[String, Any] = {
    algo.getH2OAlgorithmParams() ++
      Map(
        "nfolds" -> getNfolds(),
        "fold_column" -> getFoldCol(),
        "response_column" -> getLabelCol(),
        "weights_column" -> getWeightCol(),
        "training_frame" -> train.frameId) ++
      valid.map { fr => Map("validation_frame" -> fr.frameId) }.getOrElse(Map())
  }

  private def prepareHyperParameters(): String = {
    val it = getHyperParameters().entrySet().iterator()
    val checkedHyperParams = new java.util.HashMap[String, Array[AnyRef]]()
    while (it.hasNext) {
      val entry = it.next()
      val hyperParamName = entry.getKey

      val values = if (entry.getValue.isInstanceOf[util.ArrayList[Object]]) {
        val length = entry.getValue.asInstanceOf[util.ArrayList[_]].size()
        val arrayList = entry.getValue.asInstanceOf[util.ArrayList[_]]
        (0 until length).map(idx => arrayList.get(idx).asInstanceOf[AnyRef]).toArray
      } else {
        entry.getValue
      }
      checkedHyperParams.put(hyperParamName, values)
    }

    // REST API expects parameters without the starting `_`.
    // User in SW api should anyway specify Sparkling Water algo names and we should map it to H2O ones internally.
    // See https://0xdata.atlassian.net/browse/SW-1608
    def prepareKey(key: String) = {
      if (key.startsWith("_")) {
        key.substring(1)
      } else {
        key
      }
    }

    checkedHyperParams.asScala.map { case (key, value) =>
      s"'${prepareKey(key)}': ${stringify(value)}"
    }.mkString("{", ",", "}")
  }

  private def getGridModelKeys(gridId: String): Array[H2OModel] = {
    val conf = H2OContext.ensure().getConf
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val gridJson = withResource(readURLContent(endpoint, "GET", s"/99/Grids/$gridId", conf)) { response =>
      IOUtils.toString(response)
    }
    val gson = new Gson()
    val jsonModelIds = gson.fromJson(gridJson, classOf[JsonElement]).getAsJsonObject.get("model_ids").getAsJsonArray.asScala
    val keys = jsonModelIds.map(_.getAsJsonObject.get("name").getAsString)
    keys.map(H2OModel(_)).toArray
  }

  override def fit(dataset: Dataset[_]): H2OMOJOModel = {
    val algo = getAlgo()
    if (algo == null) {
      throw new IllegalArgumentException(s"Algorithm has to be specified. Available algorithms are " +
        s"${H2OGridSearch.SupportedAlgos.values.mkString(", ")}")
    }
    val (train, valid, internalFeatureCols) = prepareDatasetForFitting(dataset)
    val params = Map(
      "hyper_parameters" -> prepareHyperParameters(),
      "parallelism" -> getParallelism(),
      "search_criteria" -> getSearchCriteria()
    ) ++ getAlgoParams(algo, train, valid)
    val algoName = H2OGridSearch.SupportedAlgos.toH2OAlgoName(algo)
    val content = try {
      RestApiUtils.updateAsJson(s"/99/Grid/$algoName", params)
    } catch {
      case e: RestApiCommunicationException =>
        val pattern = "Illegal hyper parameter for grid search! The parameter '([A-Za-z_]+) is not gridable!".r.unanchored
        e.getMessage match {
          case pattern(parameterName) =>
            throw new IllegalArgumentException(s"Parameter '$parameterName' is not supported to be passed as hyper parameter!")
          case _ => throw e
        }
    }
    val gson = new Gson()
    val job = gson.fromJson(content, classOf[JsonElement]).getAsJsonObject.get("job").getAsJsonObject
    val jobId = job.get("key").getAsJsonObject.get("name").getAsString
    H2OJob(jobId).waitForFinish()
    val gridId = job.get("dest").getAsJsonObject.get("name").getAsString
    val unsortedGridModels = getGridModelKeys(gridId)
    if (unsortedGridModels.isEmpty) {
      throw new IllegalArgumentException("No Model returned.")
    }
    gridModels = sortGridModels(unsortedGridModels)
    gridMojoModels = gridModels.map { m =>
      val data = m.downloadMojoData()
      H2OMOJOModel.createFromMojo(data, Identifiable.randomUID(s"${algoName}_mojoModel"))
    }
    val firstModel = extractFirstModelFromGrid()
    val modelSettings = H2OMOJOSettings.createFromModelParams(this)
    H2OMOJOModel.createFromMojo(
      firstModel.downloadMojoData(),
      Identifiable.randomUID(algoName),
      modelSettings,
      internalFeatureCols)
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
      val outputParams = model.trainingParams.filter { case (key, _) => hyperParamNames.contains("_" + key) }
      Row(Seq(id) ++ outputParams.values: _*)
    }

    val colNames = gridModels.headOption.map { model =>
      val outputParams = model.trainingParams.filter { case (key, _) => hyperParamNames.contains("_" + key) }
      outputParams.keys.map(name => StructField(s"_$name", StringType, nullable = false)).toList
    }.getOrElse(List.empty)


    val schema = StructType(List(StructField("MOJO Model ID", StringType, nullable = false)) ++ colNames)
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

    val schema = StructType(List(StructField("MOJO Model ID", StringType, nullable = false)) ++ colNames)
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
}

object H2OGridSearch extends H2OParamsReadable[H2OGridSearch] {

  object SupportedAlgos extends Enumeration {
    val H2OGBM, H2OGLM, H2ODeepLearning, H2OXGBoost, H2ODRF = Value

    def checkIfSupported(algo: H2OAlgorithm[_ <: Model.Parameters]): Unit = {
      val exists = values.exists(_.toString == algo.getClass.getSimpleName)
      if (!exists) {
        throw new IllegalArgumentException(s"Grid Search is not supported for the specified algorithm '${algo.getClass}'. Supported " +
          s"algorithms are ${H2OGridSearch.SupportedAlgos.values.mkString(", ")}")
      }
    }

    def toH2OAlgoName(algo: H2OAlgorithm[_ <: Model.Parameters]): String = {
      val algoValue = values.find(_.toString == algo.getClass.getSimpleName).get
      algoValue match {
        case H2OGBM => "gbm"
        case H2OGLM => "glm"
        case H2ODeepLearning => "deeplearning"
        case H2OXGBoost => "xgboost"
        case H2ODRF => "drf"
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
  def getAlgo(): H2OSupervisedAlgorithm[_ <: Model.Parameters] = $(algo)

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
  def setAlgo(value: H2OSupervisedAlgorithm[_ <: Model.Parameters]): this.type = {
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
