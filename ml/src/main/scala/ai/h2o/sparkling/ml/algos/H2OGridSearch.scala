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
import ai.h2o.sparkling.{H2OContext, H2OFrame}
import ai.h2o.sparkling.backend.exceptions.RestApiCommunicationException
import ai.h2o.sparkling.backend.utils.{RestApiUtils, RestCommunication, RestEncodingUtils}
import ai.h2o.sparkling.ml.internals.{H2OMetric, H2OModel, H2OModelCategory}
import ai.h2o.sparkling.ml.models.{H2OMOJOModel, H2OMOJOSettings}
import ai.h2o.sparkling.ml.params.H2OGridSearchParams
import ai.h2o.sparkling.ml.utils.H2OParamsReadable
import ai.h2o.sparkling.utils.SparkSessionUtils
import hex.Model
import hex.grid.HyperSpaceSearchCriteria
import hex.schemas.GridSchemaV99
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Dataset, Row}

import scala.collection.JavaConverters._

/**
  * H2O Grid Search algorithm exposed via Spark ML pipelines.
  *
  */
class H2OGridSearch(override val uid: String)
  extends Estimator[H2OMOJOModel]
  with H2OAlgoCommonUtils
  with DefaultParamsWritable
  with H2OGridSearchParams
  with RestCommunication
  with RestEncodingUtils {

  def this() = this(Identifiable.randomUID(classOf[H2OGridSearch].getSimpleName))

  private var gridModels: Array[H2OMOJOModel] = _

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
          "max_runtime_secs" -> getMaxRuntimeSecs())
      case _ => Map("strategy" -> HyperSpaceSearchCriteria.Strategy.Cartesian.name())
    }
    criteria.map { case (key, value) => s"'$key': $value" }.mkString("{", ",", "}")
  }

  private def getAlgoParams(
      algo: H2OSupervisedAlgorithm[_ <: Model.Parameters],
      train: H2OFrame,
      valid: Option[H2OFrame]): Map[String, Any] = {
    algo.getH2OAlgorithmParams() ++
      Map(
        "nfolds" -> getAlgo().getNfolds(),
        "fold_column" -> getAlgo().getFoldCol(),
        "response_column" -> getAlgo().getLabelCol(),
        "weights_column" -> getAlgo().getWeightCol(),
        "training_frame" -> train.frameId) ++
      valid
        .map { fr =>
          Map("validation_frame" -> fr.frameId)
        }
        .getOrElse(Map())
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

    checkedHyperParams.asScala
      .map {
        case (key, value) =>
          s"'${getAlgo().getSWtoH2OParamNameMap()(key)}': ${stringify(value)}"
      }
      .mkString("{", ",", "}")
  }

  private def getGridModels(
      gridId: String,
      algoName: String,
      internalFeatureCols: Array[String]): Array[H2OMOJOModel] = {
    val conf = H2OContext.ensure().getConf
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val skippedFields = Seq((classOf[GridSchemaV99], "summary_table"), (classOf[GridSchemaV99], "scoring_history"))
    val grid = query[GridSchemaV99](endpoint, s"/99/Grids/$gridId", conf, Map.empty, skippedFields)
    val modelSettings = H2OMOJOSettings.createFromModelParams(getAlgo())
    grid.model_ids.map { modelId =>
      H2OModel(modelId.name).toMOJOModel(Identifiable.randomUID(algoName), modelSettings, internalFeatureCols)
    }
  }

  override def fit(dataset: Dataset[_]): H2OMOJOModel = {
    val algo = getAlgo()
    if (algo == null) {
      throw new IllegalArgumentException(
        s"Algorithm has to be specified. Available algorithms are " +
          s"${H2OGridSearch.SupportedAlgos.values.mkString(", ")}")
    }
    val (train, valid, internalFeatureCols) = prepareDatasetForFitting(dataset)
    val params = Map(
      "hyper_parameters" -> prepareHyperParameters(),
      "parallelism" -> getParallelism(),
      "search_criteria" -> getSearchCriteria()) ++ getAlgoParams(algo, train, valid)
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
    val unsortedGridModels = getGridModels(gridId, algoName, internalFeatureCols)
    if (unsortedGridModels.isEmpty) {
      throw new IllegalArgumentException("No model returned.")
    }
    gridModels = sortGridModels(unsortedGridModels)
    gridModels.head
  }

  private def sortGridModels(gridModels: Array[H2OMOJOModel]): Array[H2OMOJOModel] = {
    val metric = if (getSelectBestModelBy() == H2OMetric.AUTO.name()) {
      val category = H2OModelCategory.fromString(gridModels(0).getModelCategory())
      category match {
        case H2OModelCategory.Regression => H2OMetric.RMSE
        case H2OModelCategory.Binomial => H2OMetric.AUC
        case H2OModelCategory.Multinomial => H2OMetric.Logloss
      }
    } else {
      H2OMetric.valueOf(getSelectBestModelBy())
    }

    val modelMetricPair = gridModels.map { model =>
      (model, model.getCurrentMetrics().find(_._1 == metric.name()).get._2)
    }

    val ordering = if (metric.higherTheBetter) Ordering.Double.reverse else Ordering.Double
    modelMetricPair.sortBy(_._2)(ordering).map(_._1)
  }

  private def ensureGridSearchIsFitted(): Unit = {
    require(
      gridModels != null,
      "The fit method of the grid search must be called first to be able to obtain a list of models.")
  }

  def getGridModelsParams(): DataFrame = {
    ensureGridSearchIsFitted()
    val hyperParamNames = getHyperParameters().keySet().asScala.toSeq
    val h2oToSwParamMap = getAlgo().getSWtoH2OParamNameMap().map(_.swap)
    val rowValues = gridModels.zip(gridModels.map(_.uid)).map {
      case (model, id) =>
        val outputParams = extractParamsToShow(model, hyperParamNames, h2oToSwParamMap)
        Row(Seq(id) ++ outputParams.values: _*)
    }
    val colNames = gridModels.headOption
      .map { model =>
        val outputParams = extractParamsToShow(model, hyperParamNames, h2oToSwParamMap)
        outputParams.keys.map(name => StructField(name, StringType, nullable = false)).toList
      }
      .getOrElse(List.empty)
    val schema = StructType(List(StructField("MOJO Model ID", StringType, nullable = false)) ++ colNames)
    val spark = SparkSessionUtils.active
    spark.createDataFrame(spark.sparkContext.parallelize(rowValues), schema)
  }

  def getGridModelsMetrics(): DataFrame = {
    ensureGridSearchIsFitted()
    val rowValues = gridModels.zip(gridModels.map(_.uid)).map {
      case (model, id) =>
        Row(Seq(id) ++ model.getCurrentMetrics().values: _*)
    }
    val colNames = gridModels.headOption
      .map { model =>
        model.getCurrentMetrics().keys.map(StructField(_, DoubleType, nullable = false)).toList
      }
      .getOrElse(List.empty)

    val schema = StructType(List(StructField("MOJO Model ID", StringType, nullable = false)) ++ colNames)
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
      model: H2OMOJOModel,
      hyperParamNames: Seq[String],
      h2oToSwParamMap: Map[String, String]): Map[String, String] = {
    model.getTrainingParams().filter {
      case (key, _) =>
        !IgnoredParameters.all.contains(key) && hyperParamNames.contains(h2oToSwParamMap(key))
    }
  }
}

object H2OGridSearch extends H2OParamsReadable[H2OGridSearch] {

  object SupportedAlgos extends Enumeration {
    val H2OGBM, H2OGLM, H2ODeepLearning, H2OXGBoost, H2ODRF = Value

    def checkIfSupported(algo: H2OAlgorithm[_ <: Model.Parameters]): Unit = {
      val exists = values.exists(_.toString == algo.getClass.getSimpleName)
      if (!exists) {
        throw new IllegalArgumentException(
          s"Grid Search is not supported for the specified algorithm '${algo.getClass}'. Supported " +
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
