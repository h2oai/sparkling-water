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

package ai.h2o.sparkling.ml.params

import ai.h2o.sparkling.H2OFrame
import hex.Model.Parameters.{CategoricalEncodingScheme, FoldAssignmentScheme}
import hex.MultinomialAucType
import hex.ScoreKeeper.StoppingMetric
import hex.deeplearning.DeepLearningModel.DeepLearningParameters
import hex.deeplearning.DeepLearningModel.DeepLearningParameters._
import hex.genmodel.utils.DistributionFamily

trait H2OAutoEncoderParams
  extends H2OAlgoParamsBase
  with H2OAutoEncoderMOJOParams
  with HasInitialBiases
  with HasInitialWeights
  with HasIgnoredCols {

  protected def paramTag = reflect.classTag[DeepLearningParameters]

  protected val modelId = nullableStringParam(
    name = "modelId",
    doc = """Destination id for this model; auto-generated if not specified.""")

  //
  // Default values
  //
  setDefault(
    balanceClasses -> false,
    classSamplingFactors -> null,
    maxAfterBalanceSize -> 5.0f,
    activation -> Activation.Rectifier.name(),
    hidden -> Array(200, 200),
    epochs -> 10.0,
    trainSamplesPerIteration -> -2L,
    targetRatioCommToComp -> 0.05,
    seed -> -1L,
    adaptiveRate -> true,
    rho -> 0.99,
    epsilon -> 1.0e-8,
    rate -> 0.005,
    rateAnnealing -> 1.0e-6,
    rateDecay -> 1.0,
    momentumStart -> 0.0,
    momentumRamp -> 1000000.0,
    momentumStable -> 0.0,
    nesterovAcceleratedGradient -> true,
    inputDropoutRatio -> 0.0,
    hiddenDropoutRatios -> null,
    l1 -> 0.0,
    l2 -> 0.0,
    maxW2 -> 3.402823e38f,
    initialWeightDistribution -> InitialWeightDistribution.UniformAdaptive.name(),
    initialWeightScale -> 1.0,
    loss -> Loss.Automatic.name(),
    scoreInterval -> 5.0,
    scoreTrainingSamples -> 10000L,
    scoreValidationSamples -> 0L,
    scoreDutyCycle -> 0.1,
    classificationStop -> 0.0,
    regressionStop -> 1.0e-6,
    quietMode -> false,
    scoreValidationSampling -> ClassSamplingMethod.Uniform.name(),
    overwriteWithBestModel -> true,
    useAllFactorLevels -> true,
    standardize -> true,
    diagnostics -> true,
    calculateFeatureImportances -> true,
    fastMode -> true,
    forceLoadBalance -> true,
    replicateTrainingData -> true,
    singleNodeMode -> false,
    shuffleTrainingData -> false,
    missingValuesHandling -> MissingValuesHandling.MeanImputation.name(),
    sparse -> false,
    averageActivation -> 0.0,
    sparsityBeta -> 0.0,
    maxCategoricalFeatures -> 2147483647,
    reproducible -> false,
    exportWeightsAndBiases -> false,
    miniBatchSize -> 1,
    elasticAveraging -> false,
    elasticAveragingMovingRate -> 0.9,
    elasticAveragingRegularization -> 0.001,
    modelId -> null,
    nfolds -> 0,
    keepCrossValidationModels -> true,
    keepCrossValidationPredictions -> false,
    keepCrossValidationFoldAssignment -> false,
    distribution -> DistributionFamily.AUTO.name(),
    tweediePower -> 1.5,
    quantileAlpha -> 0.5,
    huberAlpha -> 0.9,
    weightCol -> null,
    foldCol -> null,
    foldAssignment -> FoldAssignmentScheme.AUTO.name(),
    categoricalEncoding -> CategoricalEncodingScheme.AUTO.name(),
    ignoreConstCols -> true,
    scoreEachIteration -> false,
    stoppingRounds -> 5,
    maxRuntimeSecs -> 0.0,
    stoppingMetric -> StoppingMetric.AUTO.name(),
    stoppingTolerance -> 0.0,
    exportCheckpointsDir -> null,
    aucType -> MultinomialAucType.AUTO.name())

  def getModelId(): String = $(modelId)

  //
  // Setters
  //
  def setInputCols(value: Array[String]): this.type = set(inputCols, value)

  def setInputCols(value: String*): this.type = setInputCols(value.toArray)

  def setOutputCol(value: String): this.type = set(outputCol, value)

  def setBalanceClasses(value: Boolean): this.type = {
    set(balanceClasses, value)
  }
           
  def setClassSamplingFactors(value: Array[Float]): this.type = {
    set(classSamplingFactors, value)
  }
           
  def setMaxAfterBalanceSize(value: Float): this.type = {
    set(maxAfterBalanceSize, value)
  }
           
  def setActivation(value: String): this.type = {
    val validated = EnumParamValidator.getValidatedEnumValue[Activation](value)
    set(activation, validated)
  }
           
  def setHidden(value: Array[Int]): this.type = {
    set(hidden, value)
  }
           
  def setEpochs(value: Double): this.type = {
    set(epochs, value)
  }
           
  def setTrainSamplesPerIteration(value: Long): this.type = {
    set(trainSamplesPerIteration, value)
  }
           
  def setTargetRatioCommToComp(value: Double): this.type = {
    set(targetRatioCommToComp, value)
  }
           
  def setSeed(value: Long): this.type = {
    set(seed, value)
  }
           
  def setAdaptiveRate(value: Boolean): this.type = {
    set(adaptiveRate, value)
  }
           
  def setRho(value: Double): this.type = {
    set(rho, value)
  }
           
  def setEpsilon(value: Double): this.type = {
    set(epsilon, value)
  }
           
  def setRate(value: Double): this.type = {
    set(rate, value)
  }
           
  def setRateAnnealing(value: Double): this.type = {
    set(rateAnnealing, value)
  }
           
  def setRateDecay(value: Double): this.type = {
    set(rateDecay, value)
  }
           
  def setMomentumStart(value: Double): this.type = {
    set(momentumStart, value)
  }
           
  def setMomentumRamp(value: Double): this.type = {
    set(momentumRamp, value)
  }
           
  def setMomentumStable(value: Double): this.type = {
    set(momentumStable, value)
  }
           
  def setNesterovAcceleratedGradient(value: Boolean): this.type = {
    set(nesterovAcceleratedGradient, value)
  }
           
  def setInputDropoutRatio(value: Double): this.type = {
    set(inputDropoutRatio, value)
  }
           
  def setHiddenDropoutRatios(value: Array[Double]): this.type = {
    set(hiddenDropoutRatios, value)
  }
           
  def setL1(value: Double): this.type = {
    set(l1, value)
  }
           
  def setL2(value: Double): this.type = {
    set(l2, value)
  }
           
  def setMaxW2(value: Float): this.type = {
    set(maxW2, value)
  }
           
  def setInitialWeightDistribution(value: String): this.type = {
    val validated = EnumParamValidator.getValidatedEnumValue[InitialWeightDistribution](value)
    set(initialWeightDistribution, validated)
  }
           
  def setInitialWeightScale(value: Double): this.type = {
    set(initialWeightScale, value)
  }
           
  def setLoss(value: String): this.type = {
    val validated = EnumParamValidator.getValidatedEnumValue[Loss](value)
    set(loss, validated)
  }
           
  def setScoreInterval(value: Double): this.type = {
    set(scoreInterval, value)
  }
           
  def setScoreTrainingSamples(value: Long): this.type = {
    set(scoreTrainingSamples, value)
  }
           
  def setScoreValidationSamples(value: Long): this.type = {
    set(scoreValidationSamples, value)
  }
           
  def setScoreDutyCycle(value: Double): this.type = {
    set(scoreDutyCycle, value)
  }
           
  def setClassificationStop(value: Double): this.type = {
    set(classificationStop, value)
  }
           
  def setRegressionStop(value: Double): this.type = {
    set(regressionStop, value)
  }
           
  def setQuietMode(value: Boolean): this.type = {
    set(quietMode, value)
  }
           
  def setScoreValidationSampling(value: String): this.type = {
    val validated = EnumParamValidator.getValidatedEnumValue[ClassSamplingMethod](value)
    set(scoreValidationSampling, validated)
  }
           
  def setOverwriteWithBestModel(value: Boolean): this.type = {
    set(overwriteWithBestModel, value)
  }

  def setUseAllFactorLevels(value: Boolean): this.type = {
    set(useAllFactorLevels, value)
  }
           
  def setStandardize(value: Boolean): this.type = {
    set(standardize, value)
  }
           
  def setDiagnostics(value: Boolean): this.type = {
    set(diagnostics, value)
  }
           
  def setCalculateFeatureImportances(value: Boolean): this.type = {
    set(calculateFeatureImportances, value)
  }
           
  def setFastMode(value: Boolean): this.type = {
    set(fastMode, value)
  }
           
  def setForceLoadBalance(value: Boolean): this.type = {
    set(forceLoadBalance, value)
  }
           
  def setReplicateTrainingData(value: Boolean): this.type = {
    set(replicateTrainingData, value)
  }
           
  def setSingleNodeMode(value: Boolean): this.type = {
    set(singleNodeMode, value)
  }
           
  def setShuffleTrainingData(value: Boolean): this.type = {
    set(shuffleTrainingData, value)
  }
           
  def setMissingValuesHandling(value: String): this.type = {
    val validated = EnumParamValidator.getValidatedEnumValue[MissingValuesHandling](value)
    set(missingValuesHandling, validated)
  }
           
  def setSparse(value: Boolean): this.type = {
    set(sparse, value)
  }
           
  def setAverageActivation(value: Double): this.type = {
    set(averageActivation, value)
  }
           
  def setSparsityBeta(value: Double): this.type = {
    set(sparsityBeta, value)
  }
           
  def setMaxCategoricalFeatures(value: Int): this.type = {
    set(maxCategoricalFeatures, value)
  }
           
  def setReproducible(value: Boolean): this.type = {
    set(reproducible, value)
  }
           
  def setExportWeightsAndBiases(value: Boolean): this.type = {
    set(exportWeightsAndBiases, value)
  }
           
  def setMiniBatchSize(value: Int): this.type = {
    set(miniBatchSize, value)
  }
           
  def setElasticAveraging(value: Boolean): this.type = {
    set(elasticAveraging, value)
  }
           
  def setElasticAveragingMovingRate(value: Double): this.type = {
    set(elasticAveragingMovingRate, value)
  }
           
  def setElasticAveragingRegularization(value: Double): this.type = {
    set(elasticAveragingRegularization, value)
  }
           
  def setModelId(value: String): this.type = {
    set(modelId, value)
  }
           
  def setNfolds(value: Int): this.type = {
    set(nfolds, value)
  }
           
  def setKeepCrossValidationModels(value: Boolean): this.type = {
    set(keepCrossValidationModels, value)
  }
           
  def setKeepCrossValidationPredictions(value: Boolean): this.type = {
    set(keepCrossValidationPredictions, value)
  }
           
  def setKeepCrossValidationFoldAssignment(value: Boolean): this.type = {
    set(keepCrossValidationFoldAssignment, value)
  }
           
  def setDistribution(value: String): this.type = {
    val validated = EnumParamValidator.getValidatedEnumValue[DistributionFamily](value)
    set(distribution, validated)
  }
           
  def setTweediePower(value: Double): this.type = {
    set(tweediePower, value)
  }
           
  def setQuantileAlpha(value: Double): this.type = {
    set(quantileAlpha, value)
  }
           
  def setHuberAlpha(value: Double): this.type = {
    set(huberAlpha, value)
  }
           
  def setWeightCol(value: String): this.type = {
    set(weightCol, value)
  }
           
  def setFoldCol(value: String): this.type = {
    set(foldCol, value)
  }
           
  def setFoldAssignment(value: String): this.type = {
    val validated = EnumParamValidator.getValidatedEnumValue[FoldAssignmentScheme](value)
    set(foldAssignment, validated)
  }
           
  def setCategoricalEncoding(value: String): this.type = {
    val validated = EnumParamValidator.getValidatedEnumValue[CategoricalEncodingScheme](value)
    set(categoricalEncoding, validated)
  }
           
  def setIgnoreConstCols(value: Boolean): this.type = {
    set(ignoreConstCols, value)
  }
           
  def setScoreEachIteration(value: Boolean): this.type = {
    set(scoreEachIteration, value)
  }
           
  def setStoppingRounds(value: Int): this.type = {
    set(stoppingRounds, value)
  }
           
  def setMaxRuntimeSecs(value: Double): this.type = {
    set(maxRuntimeSecs, value)
  }
           
  def setStoppingMetric(value: String): this.type = {
    val validated = EnumParamValidator.getValidatedEnumValue[StoppingMetric](value)
    set(stoppingMetric, validated)
  }
           
  def setStoppingTolerance(value: Double): this.type = {
    set(stoppingTolerance, value)
  }
           
  def setExportCheckpointsDir(value: String): this.type = {
    set(exportCheckpointsDir, value)
  }
           
  def setAucType(value: String): this.type = {
    val validated = EnumParamValidator.getValidatedEnumValue[MultinomialAucType](value)
    set(aucType, validated)
  }
           

  override private[sparkling] def getH2OAlgorithmParams(trainingFrame: H2OFrame): Map[String, Any] = {
    super.getH2OAlgorithmParams(trainingFrame) ++ getH2OAutoEncoderParams(trainingFrame)
  }

  private[sparkling] def getH2OAutoEncoderParams(trainingFrame: H2OFrame): Map[String, Any] = {
      Map(
        "balance_classes" -> getBalanceClasses(),
        "class_sampling_factors" -> getClassSamplingFactors(),
        "max_after_balance_size" -> getMaxAfterBalanceSize(),
        "activation" -> getActivation(),
        "hidden" -> getHidden(),
        "epochs" -> getEpochs(),
        "train_samples_per_iteration" -> getTrainSamplesPerIteration(),
        "target_ratio_comm_to_comp" -> getTargetRatioCommToComp(),
        "seed" -> getSeed(),
        "adaptive_rate" -> getAdaptiveRate(),
        "rho" -> getRho(),
        "epsilon" -> getEpsilon(),
        "rate" -> getRate(),
        "rate_annealing" -> getRateAnnealing(),
        "rate_decay" -> getRateDecay(),
        "momentum_start" -> getMomentumStart(),
        "momentum_ramp" -> getMomentumRamp(),
        "momentum_stable" -> getMomentumStable(),
        "nesterov_accelerated_gradient" -> getNesterovAcceleratedGradient(),
        "input_dropout_ratio" -> getInputDropoutRatio(),
        "hidden_dropout_ratios" -> getHiddenDropoutRatios(),
        "l1" -> getL1(),
        "l2" -> getL2(),
        "max_w2" -> getMaxW2(),
        "initial_weight_distribution" -> getInitialWeightDistribution(),
        "initial_weight_scale" -> getInitialWeightScale(),
        "loss" -> getLoss(),
        "score_interval" -> getScoreInterval(),
        "score_training_samples" -> getScoreTrainingSamples(),
        "score_validation_samples" -> getScoreValidationSamples(),
        "score_duty_cycle" -> getScoreDutyCycle(),
        "classification_stop" -> getClassificationStop(),
        "regression_stop" -> getRegressionStop(),
        "quiet_mode" -> getQuietMode(),
        "score_validation_sampling" -> getScoreValidationSampling(),
        "overwrite_with_best_model" -> getOverwriteWithBestModel(),
        "autoencoder" -> true,
        "use_all_factor_levels" -> getUseAllFactorLevels(),
        "standardize" -> getStandardize(),
        "diagnostics" -> getDiagnostics(),
        "variable_importances" -> getCalculateFeatureImportances(),
        "fast_mode" -> getFastMode(),
        "force_load_balance" -> getForceLoadBalance(),
        "replicate_training_data" -> getReplicateTrainingData(),
        "single_node_mode" -> getSingleNodeMode(),
        "shuffle_training_data" -> getShuffleTrainingData(),
        "missing_values_handling" -> getMissingValuesHandling(),
        "sparse" -> getSparse(),
        "average_activation" -> getAverageActivation(),
        "sparsity_beta" -> getSparsityBeta(),
        "max_categorical_features" -> getMaxCategoricalFeatures(),
        "reproducible" -> getReproducible(),
        "export_weights_and_biases" -> getExportWeightsAndBiases(),
        "mini_batch_size" -> getMiniBatchSize(),
        "elastic_averaging" -> getElasticAveraging(),
        "elastic_averaging_moving_rate" -> getElasticAveragingMovingRate(),
        "elastic_averaging_regularization" -> getElasticAveragingRegularization(),
        "model_id" -> getModelId(),
        "nfolds" -> getNfolds(),
        "keep_cross_validation_models" -> getKeepCrossValidationModels(),
        "keep_cross_validation_predictions" -> getKeepCrossValidationPredictions(),
        "keep_cross_validation_fold_assignment" -> getKeepCrossValidationFoldAssignment(),
        "distribution" -> getDistribution(),
        "tweedie_power" -> getTweediePower(),
        "quantile_alpha" -> getQuantileAlpha(),
        "huber_alpha" -> getHuberAlpha(),
        "weights_column" -> getWeightCol(),
        "fold_column" -> getFoldCol(),
        "fold_assignment" -> getFoldAssignment(),
        "categorical_encoding" -> getCategoricalEncoding(),
        "ignore_const_cols" -> getIgnoreConstCols(),
        "score_each_iteration" -> getScoreEachIteration(),
        "stopping_rounds" -> getStoppingRounds(),
        "max_runtime_secs" -> getMaxRuntimeSecs(),
        "stopping_metric" -> getStoppingMetric(),
        "stopping_tolerance" -> getStoppingTolerance(),
        "export_checkpoints_dir" -> getExportCheckpointsDir(),
        "auc_type" -> getAucType()) +++
      getInitialBiasesParam(trainingFrame) +++
      getInitialWeightsParam(trainingFrame) +++
      getIgnoredColsParam(trainingFrame)
  }

  override private[sparkling] def getSWtoH2OParamNameMap(): Map[String, String] = {
    super.getSWtoH2OParamNameMap() ++
      Map(
        "balanceClasses" -> "balance_classes",
        "classSamplingFactors" -> "class_sampling_factors",
        "maxAfterBalanceSize" -> "max_after_balance_size",
        "activation" -> "activation",
        "hidden" -> "hidden",
        "epochs" -> "epochs",
        "trainSamplesPerIteration" -> "train_samples_per_iteration",
        "targetRatioCommToComp" -> "target_ratio_comm_to_comp",
        "seed" -> "seed",
        "adaptiveRate" -> "adaptive_rate",
        "rho" -> "rho",
        "epsilon" -> "epsilon",
        "rate" -> "rate",
        "rateAnnealing" -> "rate_annealing",
        "rateDecay" -> "rate_decay",
        "momentumStart" -> "momentum_start",
        "momentumRamp" -> "momentum_ramp",
        "momentumStable" -> "momentum_stable",
        "nesterovAcceleratedGradient" -> "nesterov_accelerated_gradient",
        "inputDropoutRatio" -> "input_dropout_ratio",
        "hiddenDropoutRatios" -> "hidden_dropout_ratios",
        "l1" -> "l1",
        "l2" -> "l2",
        "maxW2" -> "max_w2",
        "initialWeightDistribution" -> "initial_weight_distribution",
        "initialWeightScale" -> "initial_weight_scale",
        "loss" -> "loss",
        "scoreInterval" -> "score_interval",
        "scoreTrainingSamples" -> "score_training_samples",
        "scoreValidationSamples" -> "score_validation_samples",
        "scoreDutyCycle" -> "score_duty_cycle",
        "classificationStop" -> "classification_stop",
        "regressionStop" -> "regression_stop",
        "quietMode" -> "quiet_mode",
        "scoreValidationSampling" -> "score_validation_sampling",
        "overwriteWithBestModel" -> "overwrite_with_best_model",
        "autoencoder" -> "autoencoder",
        "useAllFactorLevels" -> "use_all_factor_levels",
        "standardize" -> "standardize",
        "diagnostics" -> "diagnostics",
        "calculateFeatureImportances" -> "variable_importances",
        "fastMode" -> "fast_mode",
        "forceLoadBalance" -> "force_load_balance",
        "replicateTrainingData" -> "replicate_training_data",
        "singleNodeMode" -> "single_node_mode",
        "shuffleTrainingData" -> "shuffle_training_data",
        "missingValuesHandling" -> "missing_values_handling",
        "sparse" -> "sparse",
        "averageActivation" -> "average_activation",
        "sparsityBeta" -> "sparsity_beta",
        "maxCategoricalFeatures" -> "max_categorical_features",
        "reproducible" -> "reproducible",
        "exportWeightsAndBiases" -> "export_weights_and_biases",
        "miniBatchSize" -> "mini_batch_size",
        "elasticAveraging" -> "elastic_averaging",
        "elasticAveragingMovingRate" -> "elastic_averaging_moving_rate",
        "elasticAveragingRegularization" -> "elastic_averaging_regularization",
        "modelId" -> "model_id",
        "nfolds" -> "nfolds",
        "keepCrossValidationModels" -> "keep_cross_validation_models",
        "keepCrossValidationPredictions" -> "keep_cross_validation_predictions",
        "keepCrossValidationFoldAssignment" -> "keep_cross_validation_fold_assignment",
        "distribution" -> "distribution",
        "tweediePower" -> "tweedie_power",
        "quantileAlpha" -> "quantile_alpha",
        "huberAlpha" -> "huber_alpha",
        "weightCol" -> "weights_column",
        "offsetCol" -> "offset_column",
        "foldCol" -> "fold_column",
        "foldAssignment" -> "fold_assignment",
        "categoricalEncoding" -> "categorical_encoding",
        "ignoreConstCols" -> "ignore_const_cols",
        "scoreEachIteration" -> "score_each_iteration",
        "stoppingRounds" -> "stopping_rounds",
        "maxRuntimeSecs" -> "max_runtime_secs",
        "stoppingMetric" -> "stopping_metric",
        "stoppingTolerance" -> "stopping_tolerance",
        "exportCheckpointsDir" -> "export_checkpoints_dir",
        "aucType" -> "auc_type")
  }
      
}
    