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
import ai.h2o.sparkling.api.generation.common.IgnoredParameters
import ai.h2o.sparkling.backend.exceptions.RestApiCommunicationException
import ai.h2o.sparkling.backend.utils.{RestApiUtils, RestCommunication, RestEncodingUtils}
import ai.h2o.sparkling.ml.algos.H2OGridSearch.SupportedAlgos
import ai.h2o.sparkling.ml.internals.{H2OMetric, H2OModel, H2OModelCategory}
import ai.h2o.sparkling.ml.models.{H2OBinaryModel, H2OMOJOModel, H2OMOJOSettings}
import ai.h2o.sparkling.ml.params.H2OGridSearchParams
import ai.h2o.sparkling.ml.utils.{EstimatorCommonUtils, H2OParamsReadable}
import ai.h2o.sparkling.utils.SparkSessionUtils
import ai.h2o.sparkling.{H2OConf, H2OContext, H2OFrame}
import hex.Model
import hex.grid.HyperSpaceSearchCriteria
import hex.schemas.GridSchemaV99
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.json4s.JsonAST.JValue
import org.json4s.{JString, _}
import org.json4s.JsonDSL._
import org.json4s.jackson.JsonMethods._

import java.io.File
import scala.collection.JavaConverters._

/**
  * H2O Grid Search algorithm exposed via Spark ML pipelines.
  *
  */
class H2OGridSearch(override val uid: String)
  extends Estimator[H2OMOJOModel]
  with EstimatorCommonUtils
  with DefaultParamsWritable
  with H2OGridSearchParams
  with RestCommunication
  with RestEncodingUtils {

  def this() = this(Identifiable.randomUID(classOf[H2OGridSearch].getSimpleName))

  private var gridModels: Array[H2OMOJOModel] = _

  private var gridBinaryModels: Array[H2OBinaryModel] = Array.empty

  private def getSearchCriteria(train: H2OFrame): String = {
    val commonCriteria = getH2OGridSearchCommonCriteriaParams(train)
    val specificCriteria = HyperSpaceSearchCriteria.Strategy.valueOf(getStrategy()) match {
      case HyperSpaceSearchCriteria.Strategy.RandomDiscrete => getH2OGridSearchRandomDiscreteCriteriaParams(train)
      case _ => getH2OGridSearchCartesianCriteriaParams(train)
    }

    (commonCriteria ++ specificCriteria).map { case (key, value) => s"'$key': $value" }.mkString("{", ",", "}")
  }

  private def getAlgoParams(
      algo: H2OAlgorithm[_ <: Model.Parameters],
      train: H2OFrame,
      valid: Option[H2OFrame]): Map[String, Any] = {
    algo.getH2OAlgorithmParams(train) ++
      Map("training_frame" -> train.frameId) ++
      valid.map(fr => Map("validation_frame" -> fr.frameId)).getOrElse(Map())
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

    val algoParamMap = getAlgo().getSWtoH2OParamNameMap()
    checkedHyperParams.asScala
      .map {
        case (key, value) =>
          if (!algoParamMap.contains(key)) {
            throw new IllegalArgumentException(
              s"Hyper parameter '$key' is not a valid parameter for algorithm '${getAlgo().getClass.getSimpleName}'")
          } else {
            s"'${algoParamMap(key)}': ${stringify(value)}"
          }
      }
      .mkString("{", ",", "}")
  }

  private def getGridModels(gridId: String, algoName: String, conf: H2OConf): Array[(String, H2OMOJOModel)] = {
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val skippedFields = Seq(
      (classOf[GridSchemaV99], "cross_validation_metrics_summary"),
      (classOf[GridSchemaV99], "summary_table"),
      (classOf[GridSchemaV99], "scoring_history"))
    val grid = query[GridSchemaV99](endpoint, s"/99/Grids/$gridId", conf, Map.empty, skippedFields)
    val modelSettings = H2OMOJOSettings.createFromModelParams(getAlgo())
    val withCrossValidationModels = if (getAlgo().hasParam("keepCrossValidationModels")) {
      getAlgo().getOrDefault(getAlgo().getParam("keepCrossValidationModels")).asInstanceOf[Boolean]
    } else {
      false
    }
    grid.model_ids.map { modelId =>
      val mojoModel = H2OModel(modelId.name)
        .toMOJOModel(Identifiable.randomUID(algoName), modelSettings, withCrossValidationModels)
      (modelId.name, mojoModel)
    }
  }

  override def fit(dataset: Dataset[_]): H2OMOJOModel = {
    val algo = getAlgo()
    if (algo == null) {
      throw new IllegalArgumentException(
        s"Algorithm has to be specified. Available algorithms are " +
          s"${H2OGridSearch.SupportedAlgos.values.mkString(", ")}")
    }
    val (train, valid) = algo.prepareDatasetForFitting(dataset)
    val params = Map(
      "hyper_parameters" -> prepareHyperParameters(),
      "parallelism" -> getParallelism(),
      "search_criteria" -> getSearchCriteria(train)) ++ getAlgoParams(algo, train, valid)
    val algoName = H2OGridSearch.SupportedAlgos.toH2OAlgoName(algo)

    val gridId = try {
      trainAndGetDestinationKey(s"/99/Grid/$algoName", params)
    } catch {
      case e: RestApiCommunicationException =>
        val pattern =
          "Illegal hyper parameter for grid search! The parameter '([A-Za-z_]+) is not gridable!".r.unanchored
        e.getMessage match {
          case pattern(parameterName) =>
            throw new IllegalArgumentException(
              s"Parameter '$parameterName' is not supported to be passed as hyper parameter!")
          case _ => throw e
        }
    }
    val conf = H2OContext.ensure().getConf
    val unsortedGridModels = getGridModels(gridId, algoName, conf)
    if (unsortedGridModels.isEmpty) {
      throw new RuntimeException(
        "No model has been trained! Review the defined hyper-parameter space or try to increase " +
          "the value of the maxRuntimeSecs parameter and call the method again.")
    }
    val sortedGridModels = sortGridModels(algoName, unsortedGridModels)
    gridModels = sortedGridModels.map(_._2)

    if (algo.getKeepBinaryModels()) {
      gridBinaryModels = sortedGridModels.map {
        case (modelId, _) =>
          val downloadedModel: File = downloadBinaryModel(modelId, H2OContext.ensure().getConf)
          H2OBinaryModel.read(downloadedModel.toURI.toURL.toString, Some(modelId))
      }
    } else {
      sortedGridModels.foreach { case (modelId, _) => H2OModel(modelId).tryDelete() }
    }
    algo.deleteRegisteredH2OFrames()
    deleteRegisteredH2OFrames()
    val result: H2OMOJOModel = gridModels.head
    if (conf.isModelPrintAfterTrainingEnabled) {
      println(result)
    }
    result
  }

  def getBinaryModel(): H2OBinaryModel = {
    if (gridBinaryModels.isEmpty) {
      throw new IllegalArgumentException(
        "Algorithm needs to be fit first with the 'keepBinaryModels' parameter " +
          "set to true in order to access binary model.")
    }
    gridBinaryModels.head
  }

  private def sortGridModels(
      algoName: String,
      gridModels: Array[(String, H2OMOJOModel)]): Array[(String, H2OMOJOModel)] = {
    val metric = if (getSelectBestModelBy() == H2OMetric.AUTO.name()) {
      val category = H2OModelCategory.fromString(gridModels(0)._2.getModelCategory())
      category match {
        case H2OModelCategory.Regression => H2OMetric.RMSE
        case H2OModelCategory.CoxPH => H2OMetric.RMSE
        case H2OModelCategory.Binomial => H2OMetric.AUC
        case H2OModelCategory.AnomalyDetection => H2OMetric.Logloss
        case H2OModelCategory.Multinomial => H2OMetric.Logloss
        case H2OModelCategory.Clustering => H2OMetric.TotWithinss
      }
    } else {
      H2OMetric.valueOf(getSelectBestModelBy())
    }

    val modelMetricPair = gridModels.map { model =>
      (model, getMetricValue(model._2, metric))
    }

    val ordering = if (metric.higherTheBetter) Ordering.Double.reverse else Ordering.Double
    modelMetricPair.sortBy(_._2)(ordering).map(_._1)
  }

  private def ensureGridSearchIsFitted(): Unit = {
    require(
      gridModels != null,
      "The fit method of the grid search must be called first to be able to obtain a list of models.")
  }

  private def getMetricValue(model: H2OMOJOModel, metric: H2OMetric): Double = {
    model.getCurrentMetrics().find(_._1 == metric.name()).get._2
  }

  private def getColumnNameInModelSummaryTable(value: JValue): String = {
    val result = for {
      JObject(obj) <- value
      JField("name", JString(result)) <- obj
    } yield result
    result.head
  }

  def getGridModelsParams(): DataFrame = {
    ensureGridSearchIsFitted()
    val hyperParamNames = getHyperParameters().keySet().asScala.toSeq
    val h2oToSwParamMap = getAlgo().getSWtoH2OParamNameMap().map(_.swap)
    val algoName = SupportedAlgos.getEnumValue(getAlgo()).get.toString()
    val columnNames = gridModels.headOption
      .map { model =>
        val outputParams = extractParamsToShow(algoName, model, hyperParamNames, h2oToSwParamMap)
        outputParams.keys.toSeq
      }
      .getOrElse(Seq.empty)
    val rowValues = gridModels.map { model =>
      val outputParams = extractParamsToShow(algoName, model, hyperParamNames, h2oToSwParamMap)
      Row(Seq(model.uid) ++ columnNames.map(outputParams): _*)
    }

    val columns = columnNames.map(StructField(_, StringType, nullable = false))
    val schema = StructType(List(StructField("MOJO Model ID", StringType, nullable = false)) ++ columns)
    val spark = SparkSessionUtils.active
    spark.createDataFrame(spark.sparkContext.parallelize(rowValues), schema)
  }

  def getGridModelsMetrics(): DataFrame = {
    ensureGridSearchIsFitted()
    val columnNames = gridModels.headOption.map(_.getCurrentMetrics().keys.toSeq).getOrElse(Seq.empty)
    val rowValues = gridModels.map { model =>
      Row(Seq(model.uid) ++ columnNames.map(model.getCurrentMetrics()): _*)
    }

    val columns = columnNames.map(StructField(_, DoubleType, nullable = false))
    val schema = StructType(List(StructField("MOJO Model ID", StringType, nullable = false)) ++ columns)
    val spark = SparkSessionUtils.active
    spark.createDataFrame(spark.sparkContext.parallelize(rowValues), schema)
  }

  def getGridModels(): Array[H2OMOJOModel] = {
    ensureGridSearchIsFitted()
    gridModels
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  private def extractParamsToShow(
      algoName: String,
      model: H2OMOJOModel,
      hyperParamNames: Seq[String],
      h2oToSwParamMap: Map[String, String]): Map[String, String] = {
    model.getTrainingParams().filter {
      case (key, _) =>
        !IgnoredParameters.all(algoName).contains(key) && hyperParamNames.contains(h2oToSwParamMap(key))
    }
  }
}

object H2OGridSearch extends H2OParamsReadable[H2OGridSearch] {

  object SupportedAlgos extends Enumeration {
    val H2OGBM, H2OGLM, H2OGAM, H2ODeepLearning, H2OXGBoost, H2ODRF, H2OKMeans, H2OIsolationForest, H2OCoxPH =
      Value

    def getEnumValue(algo: H2OAlgorithm[_ <: Model.Parameters]): Option[SupportedAlgos.Value] = {
      values.find { value =>
        Array(value.toString, value.toString + "Classifier", value.toString + "Regressor")
          .contains(algo.getClass.getSimpleName)
      }
    }

    def checkIfSupported(algo: H2OAlgorithm[_ <: Model.Parameters]): Unit = {
      val exists = getEnumValue(algo).nonEmpty
      if (!exists) {
        throw new IllegalArgumentException(
          s"Grid Search is not supported for the specified algorithm '${algo.getClass}'. Supported " +
            s"algorithms are ${H2OGridSearch.SupportedAlgos.values.mkString(", ")}")
      }
    }

    def toH2OAlgoName(algo: H2OAlgorithm[_ <: Model.Parameters]): String = {
      val algoValue = getEnumValue(algo).get
      algoValue match {
        case H2OGBM => "gbm"
        case H2OGAM => "gam"
        case H2OGLM => "glm"
        case H2ODeepLearning => "deeplearning"
        case H2OXGBoost => "xgboost"
        case H2ODRF => "drf"
        case H2OCoxPH => "coxph"
        case H2OKMeans => "kmeans"
        case H2OIsolationForest => "isolationforest"
      }
    }
  }
}
