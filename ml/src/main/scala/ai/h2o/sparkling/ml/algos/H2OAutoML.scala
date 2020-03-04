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

import ai.h2o.automl.AutoMLBuildSpec.AutoMLCustomParameters
import ai.h2o.automl.{Algo, AutoML, AutoMLBuildSpec}
import ai.h2o.sparkling.backend.external.{RestApiUtils, RestCommunication}
import ai.h2o.sparkling.job.H2OJob
import ai.h2o.sparkling.ml.models.{H2OMOJOModel, H2OMOJOSettings}
import ai.h2o.sparkling.ml.params._
import ai.h2o.sparkling.ml.utils.H2OParamsReadable
import ai.h2o.sparkling.model.H2OModel
import ai.h2o.sparkling.utils.ScalaUtils.withResource
import ai.h2o.sparkling.utils.SparkSessionUtils
import com.google.gson.{Gson, JsonElement}
import hex.ScoreKeeper
import org.apache.commons.io.IOUtils
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.h2o.{Frame, H2OConf, H2OContext}
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, _}
import water.DKV
import water.support.ModelSerializationSupport

import scala.collection.JavaConverters._
import scala.util.control.NoStackTrace

/**
  * H2O AutoML algorithm exposed via Spark ML pipelines.
  */
class H2OAutoML(override val uid: String) extends Estimator[H2OMOJOModel]
  with H2OAlgoCommonUtils with DefaultParamsWritable with H2OAutoMLParams with RestCommunication {

  // Override default values
  setDefault(nfolds, 5)

  def this() = this(Identifiable.randomUID(classOf[H2OAutoML].getSimpleName))

  private var amlKeyOption: Option[String] = None

  private def getInputSpec(trainKey: String, validKey: Option[String]): Map[String, Any] = {
    Map(
      "response_column" -> getLabelCol(),
      "fold_column" -> getFoldCol(),
      "weights_column" -> getWeightCol(),
      "ignored_columns" -> getIgnoredCols(),
      "sort_metric" -> getSortMetric(),
      "training_frame" -> trainKey
    ) ++ validKey.map{ key => Map("validation_frame" -> key) }.getOrElse(Map())
  }

  private def getBuildModels(): Map[String, Any] = {
    val monotoneConstraints = getMonotoneConstraints()
    val algoParameters = if (monotoneConstraints != null && monotoneConstraints.nonEmpty) {
      Map("monotone_constrains" -> monotoneConstraints)
    } else {
      Map()
    }
    Map(
      "include_algos" -> determineIncludedAlgos(),
      "exclude_algos" -> null
    ) ++ (if (algoParameters.nonEmpty) Map("algo_parameters" -> algoParameters) else Map())
  }

  private def getBuildControl(): Map[String, Any] = {
    val stoppingCriteria = Map(
      "seed" -> getSeed(),
      "max_runtime_secs" -> getMaxRuntimeSecs(),
      "stopping_rounds" -> getStoppingRounds(),
      "stopping_tolerance" -> getStoppingTolerance(),
      "stopping_metric" -> getStoppingMetric(),
      "max_models" -> getMaxModels()
    )
    Map(
      "project_name" -> getProjectName(),
      "nfolds" -> getNfolds(),
      "balance_classes" -> getBalanceClasses(),
      "class_sampling_factors" -> getClassSamplingFactors(),
      "max_after_balance_size" -> getMaxAfterBalanceSize(),
      "keep_cross_validation_predictions" -> getKeepCrossValidationPredictions(),
      "keep_cross_validation_models" -> getKeepCrossValidationModels(),
      "stopping_criteria" -> stoppingCriteria
    )
  }

  private def fitOverRest(trainKey: String, validKey: Option[String]): Array[Byte] = {
    val inputSpec = getInputSpec(trainKey, validKey)
    val buildModels = getBuildModels()
    val buildControl = getBuildControl()
    val H2OParamNameToValueMap = Map(
      "input_spec" -> inputSpec,
      "build_models" -> buildModels,
      "build_control" -> buildControl
    )
    val conf = H2OContext.ensure().getConf
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val content = withResource(readURLContent(endpoint, "POST", s"/99/AutoMLBuilder", conf, H2OParamNameToValueMap, asJSON = true)) { response =>
      IOUtils.toString(response)
    }

    val gson = new Gson()
    val job = gson.fromJson(content, classOf[JsonElement]).getAsJsonObject.get("job").getAsJsonObject
    val jobId = job.get("key").getAsJsonObject.get("name").getAsString
    H2OJob(jobId).waitForFinish()
    val autoMLJobId = job.get("dest").getAsJsonObject.get("name").getAsString
    amlKeyOption = Some(autoMLJobId)
    val model = H2OModel(H2OAutoML.getLeaderModelId(conf, autoMLJobId))
    model.downloadMOJOData()
  }

  private def fitOverClient(trainKey: String, validKey: Option[String]): Array[Byte] = {
    val spec = new AutoMLBuildSpec
    spec.input_spec.training_frame = DKV.getGet[Frame](trainKey)._key
    spec.input_spec.validation_frame = validKey.map(DKV.getGet[Frame](_)._key).orNull
    spec.input_spec.response_column = getLabelCol()
    spec.input_spec.fold_column = getFoldCol()
    spec.input_spec.weights_column = getWeightCol()
    spec.input_spec.ignored_columns = getIgnoredCols()
    spec.input_spec.sort_metric = getSortMetric()
    spec.build_models.include_algos = determineIncludedAlgos().map(Algo.valueOf)
    spec.build_models.exclude_algos = null
    spec.build_control.project_name = getProjectName()
    spec.build_control.stopping_criteria.set_seed(getSeed())
    spec.build_control.stopping_criteria.set_max_runtime_secs(getMaxRuntimeSecs())
    spec.build_control.stopping_criteria.set_stopping_rounds(getStoppingRounds())
    spec.build_control.stopping_criteria.set_stopping_tolerance(getStoppingTolerance())
    spec.build_control.stopping_criteria.set_stopping_metric(ScoreKeeper.StoppingMetric.valueOf(getStoppingMetric()))
    spec.build_control.stopping_criteria.set_max_models(getMaxModels())
    spec.build_control.nfolds = getNfolds()
    spec.build_control.balance_classes = getBalanceClasses()
    spec.build_control.class_sampling_factors = getClassSamplingFactors()
    spec.build_control.max_after_balance_size = getMaxAfterBalanceSize()
    spec.build_control.keep_cross_validation_predictions = getKeepCrossValidationPredictions()
    spec.build_control.keep_cross_validation_models = getKeepCrossValidationModels()
    addMonotoneConstraints(spec)

    val aml = AutoML.startAutoML(spec)

    // Block until AutoML finishes
    aml.get()
    amlKeyOption = Some(aml._key.toString)
    if (aml.leader() == null) {
      throw new RuntimeException("No model returned from H2O AutoML. For example, try to ease" +
        " your 'excludeAlgo', 'maxModels' or 'maxRuntimeSecs' properties.") with NoStackTrace
    }
    val binaryModel = aml.leader()
    ModelSerializationSupport.getMojoData(binaryModel)
  }

  override def fit(dataset: Dataset[_]): H2OMOJOModel = {
    amlKeyOption = None
    val (trainKey, validKey, internalFeatureCols) = prepareDatasetForFitting(dataset)
    val mojoData = if (RestApiUtils.isRestAPIBased()) {
      fitOverRest(trainKey, validKey)
    } else {
      fitOverClient(trainKey, validKey)
    }

    val modelSettings = H2OMOJOSettings.createFromModelParams(this)
    H2OMOJOModel.createFromMojo(
      mojoData,
      Identifiable.randomUID(ModelSerializationSupport.getMojoModel(mojoData)._algoName),
      modelSettings,
      internalFeatureCols)
  }

  private def addMonotoneConstraints(spec: AutoMLBuildSpec) = {
    val monotoneConstraints = getMonotoneConstraintsAsKeyValuePairs()
    if (monotoneConstraints != null && monotoneConstraints.nonEmpty) {
      val builder = AutoMLCustomParameters.create()
      builder.add("monotone_constraints", monotoneConstraints)
      spec.build_models.algo_parameters = builder.build()
    }
  }

  private def determineIncludedAlgos(): Array[String] = {
    val bothIncludedExcluded = getIncludeAlgos().intersect(getExcludeAlgos())
    bothIncludedExcluded.foreach { algo =>
      logWarning(s"Algorithm '$algo' was specified in both include and exclude parameters. " +
        s"Excluding the algorithm.")
    }
    getIncludeAlgos().diff(bothIncludedExcluded)
  }

  private def leaderboardAsSparkFrame(amlKey: String, extraColumns: Array[String]): DataFrame = {
    if (RestApiUtils.isRestAPIBased()) {
      val hc = H2OContext.ensure()
      getLeaderboardOverRest(hc.getConf, amlKey, extraColumns)
    } else {
      getLeaderboardOverClient(amlKey, extraColumns)
    }
  }

  def getLeaderboard(extraColumns: String*): DataFrame = getLeaderboard(extraColumns.toArray)

  def getLeaderboard(extraColumns: java.util.ArrayList[String]) : DataFrame = {
    getLeaderboard(extraColumns.asScala.toArray)
  }

  def getLeaderboard(extraColumns: Array[String]): DataFrame = amlKeyOption match {
      case Some(amlKey) => leaderboardAsSparkFrame(amlKey, extraColumns)
      case None => throw new RuntimeException("The 'fit' method must be called at first!")
    }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  private def getLeaderboardOverRest(conf: H2OConf, automlId: String, extraColumns: Array[String] = Array.empty): DataFrame = {
    val endpoint = RestApiUtils.getClusterEndpoint(conf)
    val params = Map("extensions" -> extraColumns)
    val content = withResource(readURLContent(endpoint, "GET", s"/99/Leaderboards/$automlId", conf, params)) { response =>
      IOUtils.toString(response)
    }
    val gson = new Gson()
    val table = gson.fromJson(content, classOf[JsonElement]).getAsJsonObject.getAsJsonObject("table")
    import scala.collection.JavaConverters._
    val colNamesIterator = table.getAsJsonArray("columns").iterator().asScala
    val colNames = colNamesIterator.toArray.map(_.getAsJsonObject.get("name").getAsString)
    val colsData = table.getAsJsonArray("data").iterator().asScala.toArray.map(_.getAsJsonArray)
    val numRows = table.get("rowcount").getAsInt
    val rows = (0 until numRows).map { idx => Row(colsData.map(_.get(idx).getAsString): _*) }
    val spark = SparkSessionUtils.active
    val rdd = spark.sparkContext.parallelize(rows)
    val schema = StructType(colNames.map(name => StructField(name, StringType)))
    spark.createDataFrame(rdd, schema)
  }

  private def getLeaderboardOverClient(amlKey: String, extraColumns: Array[String]): DataFrame = {
    val twoDimTable = DKV.getGet[AutoML](amlKey).leaderboard().toTwoDimTable(extraColumns: _*)
    val colNames = twoDimTable.getColHeaders
    val data = twoDimTable.getCellValues.map(_.map(_.toString))
    val rows = data.map {
      Row.fromSeq(_)
    }
    val schema = StructType(colNames.map { name => StructField(name, StringType) })
    val spark = SparkSessionUtils.active
    val rdd = spark.sparkContext.parallelize(rows)
    spark.createDataFrame(rdd, schema)
  }

  private def getLeaderModelId(conf: H2OConf, automlId: String): String = {
    getLeaderboardOverRest(conf, automlId).select("model_id").head().getString(0)
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}

object H2OAutoML extends H2OParamsReadable[H2OAutoML]

trait H2OAutoMLParams extends H2OCommonSupervisedParams with HasMonotoneConstraints {

  //
  // Param definitions
  //
  private val ignoredCols = new StringArrayParam(this, "ignoredCols", "Ignored column names")
  private val includeAlgos = new StringArrayParam(this, "includeAlgos", "Algorithms to include when using automl")
  private val excludeAlgos = new StringArrayParam(this, "excludeAlgos", "Algorithms to exclude when using automl")
  private val projectName = new NullableStringParam(this, "projectName", "Identifier for models that should be grouped together in the leaderboard" +
    " (e.g., airlines and iris)")
  private val maxRuntimeSecs = new DoubleParam(this, "maxRuntimeSecs", "Maximum time in seconds for automl to be running")
  private val stoppingRounds = new IntParam(this, "stoppingRounds", "Stopping rounds")
  private val stoppingTolerance = new DoubleParam(this, "stoppingTolerance", "Stopping tolerance")
  private val stoppingMetric = new Param[String](this, "stoppingMetric", "Stopping metric")
  private val sortMetric = new Param[String](this, "sortMetric", "Sort metric for the AutoML leaderboard")
  private val balanceClasses = new BooleanParam(this, "balanceClasses", "Ballance classes")
  private val classSamplingFactors = new NullableFloatArrayParam(this, "classSamplingFactors", "Class sampling factors")
  private val maxAfterBalanceSize = new FloatParam(this, "maxAfterBalanceSize", "Max after balance size")
  private val keepCrossValidationPredictions = new BooleanParam(this, "keepCrossValidationPredictions", "Keep cross Validation predictions")
  private val keepCrossValidationModels = new BooleanParam(this, "keepCrossValidationModels", "Keep cross validation models")
  private val maxModels = new IntParam(this, "maxModels", "Maximal number of models to be trained in AutoML")

  //
  // Default values
  //
  setDefault(
    ignoredCols -> Array.empty[String],
    includeAlgos -> Algo.values().map(_.name()),
    excludeAlgos -> Array.empty[String],
    projectName -> null, // will be automatically generated
    maxRuntimeSecs -> 0.0,
    stoppingRounds -> 3,
    stoppingTolerance -> 0.001,
    stoppingMetric -> ScoreKeeper.StoppingMetric.AUTO.name(),
    sortMetric -> H2OAutoMLSortMetric.AUTO.name(),
    balanceClasses -> false,
    classSamplingFactors -> null,
    maxAfterBalanceSize -> 5.0f,
    keepCrossValidationPredictions -> false,
    keepCrossValidationModels -> false,
    maxModels -> 0
  )

  //
  // Getters
  //
  def getIgnoredCols(): Array[String] = $(ignoredCols)

  def getIncludeAlgos(): Array[String] = $(includeAlgos)

  def getExcludeAlgos(): Array[String] = $(excludeAlgos)

  def getProjectName(): String = $(projectName)

  def getMaxRuntimeSecs(): Double = $(maxRuntimeSecs)

  def getStoppingRounds(): Int = $(stoppingRounds)

  def getStoppingTolerance(): Double = $(stoppingTolerance)

  def getStoppingMetric(): String = $(stoppingMetric)

  def getSortMetric(): String = $(sortMetric)

  def getBalanceClasses(): Boolean = $(balanceClasses)

  def getClassSamplingFactors(): Array[Float] = $(classSamplingFactors)

  def getMaxAfterBalanceSize(): Float = $(maxAfterBalanceSize)

  def getKeepCrossValidationPredictions(): Boolean = $(keepCrossValidationPredictions)

  def getKeepCrossValidationModels(): Boolean = $(keepCrossValidationModels)

  def getMaxModels(): Int = $(maxModels)

  //
  // Setters
  //
  def setIgnoredCols(value: Array[String]): this.type = set(ignoredCols, value)

  def setIncludeAlgos(value: Array[String]): this.type = {
    val validated = H2OAlgoParamsHelper.getValidatedEnumValues[Algo](value, nullEnabled = true)
    set(includeAlgos, validated)
  }

  def setExcludeAlgos(value: Array[String]): this.type = {
    val validated = H2OAlgoParamsHelper.getValidatedEnumValues[Algo](value, nullEnabled = true)
    set(excludeAlgos, validated)
  }

  def setProjectName(value: String): this.type = set(projectName, value)

  def setMaxRuntimeSecs(value: Double): this.type = set(maxRuntimeSecs, value)

  def setStoppingRounds(value: Int): this.type = set(stoppingRounds, value)

  def setStoppingTolerance(value: Double): this.type = set(stoppingTolerance, value)

  def setStoppingMetric(value: String): this.type = {
    val validated = H2OAlgoParamsHelper.getValidatedEnumValue[ScoreKeeper.StoppingMetric](value)
    set(stoppingMetric, validated)
  }

  def setSortMetric(value: String): this.type = {
    val validated = H2OAlgoParamsHelper.getValidatedEnumValue[H2OAutoMLSortMetric](value)
    set(sortMetric, validated)
  }

  def setBalanceClasses(value: Boolean): this.type = set(balanceClasses, value)

  def setClassSamplingFactors(value: Array[Float]): this.type = set(classSamplingFactors, value)

  def setMaxAfterBalanceSize(value: Float): this.type = set(maxAfterBalanceSize, value)

  def setKeepCrossValidationPredictions(value: Boolean): this.type = set(keepCrossValidationPredictions, value)

  def setKeepCrossValidationModels(value: Boolean): this.type = set(keepCrossValidationModels, value)

  def setMaxModels(value: Int): this.type = set(maxModels, value)
}
