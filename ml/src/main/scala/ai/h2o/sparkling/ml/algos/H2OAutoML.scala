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

import ai.h2o.sparkling.{H2OContext, H2OFrame}
import ai.h2o.sparkling.backend.utils.{RestApiUtils, RestCommunication}
import ai.h2o.sparkling.ml.internals.H2OModel
import ai.h2o.sparkling.ml.models.{H2OMOJOModel, H2OMOJOSettings}
import ai.h2o.sparkling.ml.params._
import ai.h2o.sparkling.ml.utils.H2OParamsReadable
import ai.h2o.sparkling.utils.ScalaUtils.withResource
import ai.h2o.sparkling.utils.SparkSessionUtils
import com.google.gson.{Gson, JsonElement}
import org.apache.commons.io.IOUtils
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, _}

import scala.collection.JavaConverters._
import scala.util.control.NoStackTrace

/**
  * H2O AutoML algorithm exposed via Spark ML pipelines.
  */
class H2OAutoML(override val uid: String)
  extends Estimator[H2OMOJOModel]
  with H2OAlgoCommonUtils
  with DefaultParamsWritable
  with H2OAutoMLParams
  with RestCommunication {

  def this() = this(Identifiable.randomUID(classOf[H2OAutoML].getSimpleName))

  private var amlKeyOption: Option[String] = None

  private def getInputSpec(train: H2OFrame, valid: Option[H2OFrame]): Map[String, Any] = {
    getH2OAutoMLInputParams() ++
      Map("training_frame" -> train.frameId) ++
      valid.map(fr => Map("validation_frame" -> fr.frameId)).getOrElse(Map())
  }

  private def getBuildModels(): Map[String, Any] = {
    val monotoneConstraints = getMonotoneConstraints()
    val algoParameters = if (monotoneConstraints != null && monotoneConstraints.nonEmpty) {
      Map("monotone_constrains" -> monotoneConstraints)
    } else {
      Map()
    }
    val extra = if (algoParameters.nonEmpty) Map("algo_parameters" -> algoParameters) else Map()

    // Removing "include_algos", "exclude_algos" from s H2OAutoMLBuildModelsParams since an effective set algorithms
    // needs to be calculated and stored into "include_algos". The "exclude_algos" are then reset to null and both
    // altered parameters are added to the result.
    val essentialParameters = getH2OAutoMLBuildModelsParams() - ("include_algos", "exclude_algos")

    essentialParameters ++ Map("include_algos" -> determineIncludedAlgos(), "exclude_algos" -> null) ++ extra
  }

  private def getBuildControl(): Map[String, Any] = {
    val stoppingCriteria = getH2OAutoMLStoppingCriteriaParams()
    getH2OAutoMLBuildControlParams() + ("stopping_criteria" -> stoppingCriteria)
  }

  override def fit(dataset: Dataset[_]): H2OMOJOModel = {
    amlKeyOption = None
    val (trainKey, validKey, internalFeatureCols) = prepareDatasetForFitting(dataset)
    val inputSpec = getInputSpec(trainKey, validKey)
    val buildModels = getBuildModels()
    val buildControl = getBuildControl()
    val params = Map("input_spec" -> inputSpec, "build_models" -> buildModels, "build_control" -> buildControl)
    val autoMLId = trainAndGetDestinationKey(s"/99/AutoMLBuilder", params, encodeParamsAsJson = true)
    amlKeyOption = Some(autoMLId)

    val algoName = getLeaderboard().select("model_id").head().getString(0)
    deleteRegisteredH2OFrames()
    H2OModel(getLeaderModelId(autoMLId))
      .toMOJOModel(Identifiable.randomUID(algoName), H2OMOJOSettings.createFromModelParams(this), internalFeatureCols)
  }

  private def determineIncludedAlgos(): Array[String] = {
    val bothIncludedExcluded = getIncludeAlgos().intersect(Option(getExcludeAlgos()).getOrElse(Array.empty))
    bothIncludedExcluded.foreach { algo =>
      logWarning(
        s"Algorithm '$algo' was specified in both include and exclude parameters. " +
          s"Excluding the algorithm.")
    }
    getIncludeAlgos().diff(bothIncludedExcluded)
  }

  def getLeaderboard(extraColumns: String*): DataFrame = getLeaderboard(extraColumns.toArray)

  def getLeaderboard(extraColumns: java.util.ArrayList[String]): DataFrame = {
    getLeaderboard(extraColumns.asScala.toArray)
  }

  def getLeaderboard(extraColumns: Array[String]): DataFrame = amlKeyOption match {
    case Some(amlKey) => getLeaderboard(amlKey, extraColumns)
    case None => throw new RuntimeException("The 'fit' method must be called at first!")
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  private def getLeaderboard(automlId: String, extraColumns: Array[String] = Array.empty): DataFrame = {
    val params = Map("extensions" -> extraColumns)
    val conf = H2OContext.ensure().getConf
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val content = withResource(
      readURLContent(endpoint, "GET", s"/99/Leaderboards/$automlId", conf, params, encodeParamsAsJson = false, None)) {
      response =>
        IOUtils.toString(response)
    }
    val gson = new Gson()
    val table = gson.fromJson(content, classOf[JsonElement]).getAsJsonObject.getAsJsonObject("table")
    val colNamesIterator = table.getAsJsonArray("columns").iterator().asScala
    val colNames = colNamesIterator.toArray.map(_.getAsJsonObject.get("name").getAsString)
    val colsData = table.getAsJsonArray("data").iterator().asScala.toArray.map(_.getAsJsonArray)
    val numRows = table.get("rowcount").getAsInt
    val rows = (0 until numRows).map { idx =>
      Row(colsData.map(_.get(idx).getAsString): _*)
    }
    val spark = SparkSessionUtils.active
    val rdd = spark.sparkContext.parallelize(rows)
    val schema = StructType(colNames.map(name => StructField(name, StringType)))
    spark.createDataFrame(rdd, schema)
  }

  private def getLeaderModelId(automlId: String): String = {
    val leaderBoard = getLeaderboard(automlId).select("model_id")
    if (leaderBoard.count() == 0) {
      throw new RuntimeException(
        "No model returned from H2O AutoML. For example, try to ease" +
          " your 'excludeAlgo', 'maxModels' or 'maxRuntimeSecs' properties.") with NoStackTrace
    } else {}
    leaderBoard.head().getString(0)
  }

  override private[sparkling] def getExcludedCols(): Seq[String] = {
    super.getExcludedCols() ++ Seq(getLabelCol(), getFoldCol(), getWeightCol())
      .flatMap(Option(_)) // Remove nulls
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}

object H2OAutoML extends H2OParamsReadable[H2OAutoML]
