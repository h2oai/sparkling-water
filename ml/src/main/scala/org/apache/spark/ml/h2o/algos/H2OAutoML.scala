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

import java.util.Date

import ai.h2o.automl.{Algo, AutoML, AutoMLBuildSpec}
import hex.ScoreKeeper
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.h2o._
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.h2o.models.H2OMOJOModel
import org.apache.spark.ml.h2o.param._
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.{StringType, StructField, StructType}
import org.apache.spark.sql.{Dataset, _}
import water.Key
import water.support.{H2OFrameSupport, ModelSerializationSupport}
import water.util.DeprecatedMethod

import scala.util.Random
import scala.util.control.NoStackTrace

/**
  * H2O AutoML pipeline step
  */
class H2OAutoML(override val uid: String) extends Estimator[H2OMOJOModel]
  with DefaultParamsWritable with H2OAutoMLParams {

  private lazy val hc = H2OContext.getOrCreate(SparkSession.builder().getOrCreate())

  def this() = this(Identifiable.randomUID("automl"))

  var leaderboard: Option[DataFrame] = None

  override def fit(dataset: Dataset[_]): H2OMOJOModel = {
    val spec = new AutoMLBuildSpec

    // override the buildSpec with the configuration specified directly on the estimator
    if (getProjectName() == null) {
      // generate random name to generate fresh leaderboard (the default behaviour)
      setProjectName(Random.alphanumeric.take(30).mkString)
    }
    val input = hc.asH2OFrame(dataset.toDF())
    // check if we need to do any splitting
    if (getRatio() < 1.0) {
      // need to do splitting
      val keys = H2OFrameSupport.split(input, Seq(Key.rand(), Key.rand()), Seq(getRatio()))
      spec.input_spec.training_frame = keys(0)._key
      if (keys.length > 1) {
        spec.input_spec.validation_frame = keys(1)._key
      }
    } else {
      spec.input_spec.training_frame = input._key
    }

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
    spec.build_models.exclude_algos = if (getExcludeAlgos() == null) null else Array(getExcludeAlgos(): _*)
    spec.build_models.include_algos = if (getIncludeAlgos() == null) null else Array(getIncludeAlgos(): _*)
    spec.build_control.project_name = getProjectName()
    spec.build_control.stopping_criteria.set_seed(getSeed())
    spec.build_control.stopping_criteria.set_max_runtime_secs(getMaxRuntimeSecs())
    spec.build_control.stopping_criteria.set_stopping_rounds(getStoppingRounds())
    spec.build_control.stopping_criteria.set_stopping_tolerance(getStoppingTolerance())
    spec.build_control.stopping_criteria.set_stopping_metric(getStoppingMetric())
    spec.build_control.stopping_criteria.set_max_models(getMaxModels())
    spec.build_control.nfolds = getNfolds()
    spec.build_control.balance_classes = getBalanceClasses()
    spec.build_control.class_sampling_factors = getClassSamplingFactors()
    spec.build_control.max_after_balance_size = getMaxAfterBalanceSize()
    spec.build_control.keep_cross_validation_predictions = getKeepCrossValidationPredictions()
    spec.build_control.keep_cross_validation_models = getKeepCrossValidationModels()
    water.DKV.put(trainFrame)
    val aml = new AutoML(Key.make(uid), new Date(), spec)
    AutoML.startAutoML(aml)
    // Block until AutoML finishes
    aml.get()

    leaderboard = leaderboardAsSparkFrame(aml)
    if (aml.leader() == null) {
      throw new RuntimeException("No model returned from H2O AutoML. For example, try to ease" +
        " your 'excludeAlgo', 'maxModels' or 'maxRuntimeSecs' properties.") with NoStackTrace
    }
    val model = trainModel(aml)
    model.setConvertUnknownCategoricalLevelsToNa(true)
    model
  }

  private def leaderboardAsSparkFrame(aml: AutoML): Option[DataFrame] = {
    // Get LeaderBoard
    val twoDimtable = aml.leaderboard().toTwoDimTable
    val colNames = twoDimtable.getColHeaders
    val data = aml.leaderboard().toTwoDimTable.getCellValues.map(_.map(_.toString))
    val rows = data.map {
      Row.fromSeq(_)
    }
    val schema = StructType(colNames.map { name => StructField(name, StringType) })
    val rdd = hc.sparkContext.parallelize(rows)
    Some(hc.sparkSession.createDataFrame(rdd, schema))
  }

  def trainModel(aml: AutoML) = {
    new H2OMOJOModel(ModelSerializationSupport.getMojoData(aml.leader()))
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)
}

object H2OAutoML extends DefaultParamsReadable[py_sparkling.ml.algos.H2OAutoML]

trait H2OAutoMLParams extends DeprecatableParams {

  override protected def renamingMap: Map[String, String] = Map(
    "predictionCol" -> "labelCol",
    "foldColumn" -> "foldCol",
    "ignoredColumns" -> "ignoredCols"
  )

  //
  // Param definitions
  //
  private val labelCol = new Param[String](this, "labelCol", "Label column name")
  private val allStringColumnsToCategorical = new BooleanParam(this, "allStringColumnsToCategorical", "Transform all strings columns to categorical")
  private val columnsToCategorical = new StringArrayParam(this, "columnsToCategorical", "List of columns to convert to categoricals before modelling")
  private val ratio = new DoubleParam(this, "ratio", "Determines in which ratios split the dataset")
  private val foldCol = new NullableStringParam(this, "foldCol", "Fold column name")
  private val weightCol = new NullableStringParam(this, "weightCol", "Weight column name")
  private val ignoredCols = new StringArrayParam(this, "ignoredCols", "Ignored column names")
  private val includeAlgos = new H2OAutoMLAlgosParam(this, "includeAlgos", "Algorithms to include when using automl")
  private val excludeAlgos = new H2OAutoMLAlgosParam(this, "excludeAlgos", "Algorithms to exclude when using automl")
  private val projectName = new NullableStringParam(this, "projectName", "Identifier for models that should be grouped together in the leaderboard" +
    " (e.g., airlines and iris)")
  private val maxRuntimeSecs = new DoubleParam(this, "maxRuntimeSecs", "Maximum time in seconds for automl to be running")
  private val stoppingRounds = new IntParam(this, "stoppingRounds", "Stopping rounds")
  private val stoppingTolerance = new DoubleParam(this, "stoppingTolerance", "Stopping tolerance")
  private val stoppingMetric = new StoppingMetricParam(this, "stoppingMetric", "Stopping metric")
  private val nfolds = new IntParam(this, "nfolds", "Cross-validation fold construction")
  private val convertUnknownCategoricalLevelsToNa = new BooleanParam(this, "convertUnknownCategoricalLevelsToNa", "Convert unknown" +
    " categorical levels to NA during predictions")
  private val seed = new IntParam(this, "seed", "seed")
  private val sortMetric = new NullableStringParam(this, "sortMetric", "Sort metric for the AutoML leaderboard")
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
    labelCol -> "label",
    allStringColumnsToCategorical -> true,
    columnsToCategorical -> Array.empty[String],
    ratio -> 1.0, // 1.0 means use whole frame as training frame,
    foldCol -> null,
    weightCol -> null,
    ignoredCols -> Array.empty[String],
    includeAlgos -> null,
    excludeAlgos -> null,
    projectName -> null, // will be automatically generated
    maxRuntimeSecs -> 3600,
    stoppingRounds -> 3,
    stoppingTolerance -> 0.001,
    stoppingMetric -> ScoreKeeper.StoppingMetric.AUTO,
    nfolds -> 5,
    convertUnknownCategoricalLevelsToNa -> false,
    seed -> -1, // true random
    sortMetric -> null,
    balanceClasses -> false,
    classSamplingFactors -> null,
    maxAfterBalanceSize -> 5.0f,
    keepCrossValidationPredictions -> true,
    keepCrossValidationModels -> true,
    maxModels -> 0
  )

  //
  // Getters
  //
  @DeprecatedMethod("getLabelCol")
  def getPredictionCol(): String = getLabelCol()

  def getLabelCol(): String = $(labelCol)

  def getAllStringColumnsToCategorical(): Boolean = $(allStringColumnsToCategorical)

  def getColumnsToCategorical(): Array[String] = $(columnsToCategorical)

  def getRatio(): Double = $(ratio)

  def getFoldCol() = $(foldCol)

  @DeprecatedMethod("getFoldCol")
  def getFoldColumn() = getFoldCol()

  def getWeightCol(): String = $(weightCol)

  @DeprecatedMethod("getWeightCol")
  def getWeightsColumn(): String = getWeightCol()

  def getIgnoredCols(): Array[String] = $(ignoredCols)

  @DeprecatedMethod("getIgnoredCols")
  def getIgnoredColumns(): Array[String] = getIgnoredCols()

  def getIncludeAlgos(): Array[Algo] = $(includeAlgos)

  def getExcludeAlgos(): Array[Algo] = $(excludeAlgos)

  def getProjectName(): String = $(projectName)

  def getMaxRuntimeSecs(): Double = $(maxRuntimeSecs)

  def getStoppingRounds(): Int = $(stoppingRounds)

  def getStoppingTolerance(): Double = $(stoppingTolerance)

  def getStoppingMetric(): ScoreKeeper.StoppingMetric = $(stoppingMetric)

  def getNfolds(): Int = $(nfolds)

  def getConvertUnknownCategoricalLevelsToNa(): Boolean = $(convertUnknownCategoricalLevelsToNa)

  def getSeed(): Int = $(seed)

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
  @DeprecatedMethod("setLabelCol")
  def setPredictionCol(value: String): this.type = setLabelCol(value)

  def setLabelCol(value: String): this.type = set(labelCol, value)

  def setAllStringColumnsToCategorical(value: Boolean): this.type = set(allStringColumnsToCategorical, value)

  def setColumnsToCategorical(first: String, others: String*): this.type = set(columnsToCategorical, Array(first) ++ others)

  def setColumnsToCategorical(columns: Array[String]): this.type = set(columnsToCategorical, columns)

  def setRatio(value: Double): this.type = set(ratio, value)

  def setFoldCol(value: String): this.type = set(foldCol, value)

  @DeprecatedMethod("setFoldCol")
  def setFoldColumn(value: String): this.type = setFoldCol(value)

  def setWeightCol(value: String): this.type = set(weightCol, value)

  @DeprecatedMethod("setWeightCol")
  def setWeightsColumn(value: String): this.type = setWeightCol(value)

  def setIgnoredCols(value: Array[String]): this.type = set(ignoredCols, value)

  @DeprecatedMethod("setIgnoredCols")
  def setIgnoredColumns(value: Array[String]): this.type = setIgnoredCols(value)

  def setIncludeAlgos(value: Array[ai.h2o.automl.Algo]): this.type = set(includeAlgos, value)

  def setExcludeAlgos(value: Array[ai.h2o.automl.Algo]): this.type = set(excludeAlgos, value)

  def setProjectName(value: String): this.type = set(projectName, value)

  def setMaxRuntimeSecs(value: Double): this.type = set(maxRuntimeSecs, value)

  def setStoppingRounds(value: Int): this.type = set(stoppingRounds, value)

  def setStoppingTolerance(value: Double): this.type = set(stoppingTolerance, value)

  def setStoppingMetric(value: ScoreKeeper.StoppingMetric): this.type = set(stoppingMetric, value)

  def setNfolds(value: Int): this.type = set(nfolds, value)

  def setConvertUnknownCategoricalLevelsToNa(value: Boolean): this.type = set(convertUnknownCategoricalLevelsToNa, value)

  def setSeed(value: Int): this.type = set(seed, value)

  def setSortMetric(value: String): this.type = {
    val allowedValues = Seq("AUTO", "deviance", "logloss", "MSE", "RMSE", "MAE", "RMSLE", "AUC", "mean_per_class_error")
    if (!allowedValues.contains(value)) {
      throw new IllegalArgumentException(s"Allowed values for AutoML Stopping Metric are: ${allowedValues.mkString(", ")}")
    }
    if (value == "AUTO") {
      set(sortMetric, null)
    } else {
      set(sortMetric, value)
    }
  }

  def setBalanceClasses(value: Boolean): this.type = set(balanceClasses, value)

  def setClassSamplingFactors(value: Array[Float]): this.type = set(classSamplingFactors, value)

  def setMaxAfterBalanceSize(value: Float): this.type = set(maxAfterBalanceSize, value)

  def setKeepCrossValidationPredictions(value: Boolean): this.type = set(keepCrossValidationPredictions, value)

  def setKeepCrossValidationModels(value: Boolean): this.type = set(keepCrossValidationModels, value)

  def setMaxModels(value: Int): this.type = set(maxModels, value)
}

class H2OAutoMLAlgosParam private[h2o](parent: Params, name: String, doc: String,
                                       isValid: Array[ai.h2o.automl.Algo] => Boolean)
  extends EnumArrayParam[ai.h2o.automl.Algo](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)
}


