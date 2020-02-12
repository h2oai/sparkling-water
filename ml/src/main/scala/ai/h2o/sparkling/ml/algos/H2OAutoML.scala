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
import ai.h2o.sparkling.ml.models.{H2OMOJOModel, H2OMOJOSettings}
import ai.h2o.sparkling.ml.params._
import ai.h2o.sparkling.ml.utils.H2OParamsReadable
import hex.ScoreKeeper
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.h2o.Frame
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, _}
import water.DKV
import water.support.{H2OFrameSupport, ModelSerializationSupport}

import scala.util.control.NoStackTrace

/**
  * H2O AutoML algorithm exposed via Spark ML pipelines.
  */
class H2OAutoML(override val uid: String) extends Estimator[H2OMOJOModel]
  with H2OAlgoCommonUtils with DefaultParamsWritable with H2OAutoMLParams {

  // Override default values
  setDefault(nfolds, 5)

  private lazy val spark = SparkSession.builder().getOrCreate()

  def this() = this(Identifiable.randomUID(classOf[H2OAutoML].getSimpleName))

  var leaderboard: Option[DataFrame] = None

  override def fit(dataset: Dataset[_]): H2OMOJOModel = {
    val spec = new AutoMLBuildSpec

    val (trainKey, validKey, internalFeatureCols) = prepareDatasetForFitting(dataset)
    spec.input_spec.training_frame = DKV.getGet[Frame](trainKey)._key
    spec.input_spec.validation_frame = validKey.map(DKV.getGet[Frame](_)._key).orNull

    val trainFrame = spec.input_spec.training_frame.get()
    if (getAllStringColumnsToCategorical()) {
      H2OFrameSupport.allStringVecToCategorical(trainFrame)
    }
    H2OFrameSupport.columnsToCategorical(trainFrame, getColumnsToCategorical())

    spec.input_spec.response_column = getLabelCol()
    spec.input_spec.fold_column = getFoldCol()
    spec.input_spec.weights_column = getWeightCol()
    spec.input_spec.ignored_columns = getIgnoredCols()
    spec.input_spec.sort_metric = getSortMetric()
    spec.build_models.include_algos = determineIncludedAlgos()
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

    leaderboard = leaderboardAsSparkFrame(aml)
    if (aml.leader() == null) {
      throw new RuntimeException("No model returned from H2O AutoML. For example, try to ease" +
        " your 'excludeAlgo', 'maxModels' or 'maxRuntimeSecs' properties.") with NoStackTrace
    }

    val binaryModel = aml.leader()
    val mojoData = ModelSerializationSupport.getMojoData(binaryModel)
    val modelSettings = H2OMOJOSettings.createFromModelParams(this)
    H2OMOJOModel.createFromMojo(
      mojoData,
      Identifiable.randomUID(aml.leader()._parms.algoName()),
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

  private def determineIncludedAlgos(): Array[Algo] = {
    val bothIncludedExcluded = getIncludeAlgos().intersect(getExcludeAlgos())
    bothIncludedExcluded.foreach { algo =>
      logWarning(s"Algorithm '$algo' was specified in both include and exclude parameters. " +
        s"Excluding the algorithm.")
    }
    getIncludeAlgos().diff(bothIncludedExcluded).map(Algo.valueOf)
  }

  private def leaderboardAsSparkFrame(aml: AutoML): Option[DataFrame] = {
    // Get LeaderBoard
    val twoDimtable = aml.leaderboard().toTwoDimTable()
    val colNames = twoDimtable.getColHeaders
    val data = aml.leaderboard().toTwoDimTable().getCellValues.map(_.map(_.toString))
    val rows = data.map {
      Row.fromSeq(_)
    }
    val schema = StructType(colNames.map { name => StructField(name, StringType) })
    val rdd = spark.sparkContext.parallelize(rows)
    Some(spark.createDataFrame(rdd, schema))
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    schema
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
