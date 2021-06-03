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

import ai.h2o.sparkling.ml.models.SpecificMOJOParameters
import hex.genmodel.MojoModel
import org.apache.spark.expose.Logging
import org.apache.spark.ml.param.Param

trait H2OAutoEncoderMOJOParams extends ParameterConstructorMethods with SpecificMOJOParameters with Logging {

  //
  // Parameter definitions
  //
  protected val inputCols = new NullableStringArrayParam(this, "inputCol", "input column names")

  protected val outputCol = new Param[String](this, "outputCol", "output column name")


  protected val balanceClasses = booleanParam(
    name = "balanceClasses",
    doc = """Balance training data class counts via over/under-sampling (for imbalanced data).""")

  protected val classSamplingFactors = nullableFloatArrayParam(
    name = "classSamplingFactors",
    doc = """Desired over/under-sampling ratios per class (in lexicographic order). If not specified, sampling factors will be automatically computed to obtain class balance during training. Requires balance_classes.""")

  protected val maxAfterBalanceSize = floatParam(
    name = "maxAfterBalanceSize",
    doc = """Maximum relative size of the training data after balancing class counts (can be less than 1.0). Requires balance_classes.""")

  protected val activation = nullableStringParam(
    name = "activation",
    doc = """Activation function. Possible values are ``"Tanh"``, ``"TanhWithDropout"``, ``"Rectifier"``, ``"RectifierWithDropout"``, ``"Maxout"``, ``"MaxoutWithDropout"``, ``"ExpRectifier"``, ``"ExpRectifierWithDropout"``.""")

  protected val hidden = intArrayParam(
    name = "hidden",
    doc = """Hidden layer sizes (e.g. [100, 100]).""")

  protected val epochs = doubleParam(
    name = "epochs",
    doc = """How many times the dataset should be iterated (streamed), can be fractional.""")

  protected val trainSamplesPerIteration = longParam(
    name = "trainSamplesPerIteration",
    doc = """Number of training samples (globally) per MapReduce iteration. Special values are 0: one epoch, -1: all available data (e.g., replicated training data), -2: automatic.""")

  protected val targetRatioCommToComp = doubleParam(
    name = "targetRatioCommToComp",
    doc = """Target ratio of communication overhead to computation. Only for multi-node operation and train_samples_per_iteration = -2 (auto-tuning).""")

  protected val seed = longParam(
    name = "seed",
    doc = """Seed for random numbers (affects sampling) - Note: only reproducible when running single threaded.""")

  protected val adaptiveRate = booleanParam(
    name = "adaptiveRate",
    doc = """Adaptive learning rate.""")

  protected val rho = doubleParam(
    name = "rho",
    doc = """Adaptive learning rate time decay factor (similarity to prior updates).""")

  protected val epsilon = doubleParam(
    name = "epsilon",
    doc = """Adaptive learning rate smoothing factor (to avoid divisions by zero and allow progress).""")

  protected val rate = doubleParam(
    name = "rate",
    doc = """Learning rate (higher => less stable, lower => slower convergence).""")

  protected val rateAnnealing = doubleParam(
    name = "rateAnnealing",
    doc = """Learning rate annealing: rate / (1 + rate_annealing * samples).""")

  protected val rateDecay = doubleParam(
    name = "rateDecay",
    doc = """Learning rate decay factor between layers (N-th layer: rate * rate_decay ^ (n - 1).""")

  protected val momentumStart = doubleParam(
    name = "momentumStart",
    doc = """Initial momentum at the beginning of training (try 0.5).""")

  protected val momentumRamp = doubleParam(
    name = "momentumRamp",
    doc = """Number of training samples for which momentum increases.""")

  protected val momentumStable = doubleParam(
    name = "momentumStable",
    doc = """Final momentum after the ramp is over (try 0.99).""")

  protected val nesterovAcceleratedGradient = booleanParam(
    name = "nesterovAcceleratedGradient",
    doc = """Use Nesterov accelerated gradient (recommended).""")

  protected val inputDropoutRatio = doubleParam(
    name = "inputDropoutRatio",
    doc = """Input layer dropout ratio (can improve generalization, try 0.1 or 0.2).""")

  protected val hiddenDropoutRatios = nullableDoubleArrayParam(
    name = "hiddenDropoutRatios",
    doc = """Hidden layer dropout ratios (can improve generalization), specify one value per hidden layer, defaults to 0.5.""")

  protected val l1 = doubleParam(
    name = "l1",
    doc = """L1 regularization (can add stability and improve generalization, causes many weights to become 0).""")

  protected val l2 = doubleParam(
    name = "l2",
    doc = """L2 regularization (can add stability and improve generalization, causes many weights to be small.""")

  protected val maxW2 = floatParam(
    name = "maxW2",
    doc = """Constraint for squared sum of incoming weights per unit (e.g. for Rectifier).""")

  protected val initialWeightDistribution = nullableStringParam(
    name = "initialWeightDistribution",
    doc = """Initial weight distribution. Possible values are ``"UniformAdaptive"``, ``"Uniform"``, ``"Normal"``.""")

  protected val initialWeightScale = doubleParam(
    name = "initialWeightScale",
    doc = """Uniform: -value...value, Normal: stddev.""")

  protected val loss = nullableStringParam(
    name = "loss",
    doc = """Loss function. Possible values are ``"Automatic"``, ``"Quadratic"``, ``"CrossEntropy"``, ``"ModifiedHuber"``, ``"Huber"``, ``"Absolute"``, ``"Quantile"``.""")

  protected val scoreInterval = doubleParam(
    name = "scoreInterval",
    doc = """Shortest time interval (in seconds) between model scoring.""")

  protected val scoreTrainingSamples = longParam(
    name = "scoreTrainingSamples",
    doc = """Number of training set samples for scoring (0 for all).""")

  protected val scoreValidationSamples = longParam(
    name = "scoreValidationSamples",
    doc = """Number of validation set samples for scoring (0 for all).""")

  protected val scoreDutyCycle = doubleParam(
    name = "scoreDutyCycle",
    doc = """Maximum duty cycle fraction for scoring (lower: more training, higher: more scoring).""")

  protected val classificationStop = doubleParam(
    name = "classificationStop",
    doc = """Stopping criterion for classification error fraction on training data (-1 to disable).""")

  protected val regressionStop = doubleParam(
    name = "regressionStop",
    doc = """Stopping criterion for regression error (MSE) on training data (-1 to disable).""")

  protected val quietMode = booleanParam(
    name = "quietMode",
    doc = """Enable quiet mode for less output to standard output.""")

  protected val scoreValidationSampling = nullableStringParam(
    name = "scoreValidationSampling",
    doc = """Method used to sample validation dataset for scoring. Possible values are ``"Uniform"``, ``"Stratified"``.""")

  protected val overwriteWithBestModel = booleanParam(
    name = "overwriteWithBestModel",
    doc = """If enabled, override the final model with the best model found during training.""")

  protected val useAllFactorLevels = booleanParam(
    name = "useAllFactorLevels",
    doc = """Use all factor levels of categorical variables. Otherwise, the first factor level is omitted (without loss of accuracy). Useful for variable importances and auto-enabled for autoencoder.""")

  protected val standardize = booleanParam(
    name = "standardize",
    doc = """If enabled, automatically standardize the data. If disabled, the user must provide properly scaled input data.""")

  protected val diagnostics = booleanParam(
    name = "diagnostics",
    doc = """Enable diagnostics for hidden layers.""")

  protected val calculateFeatureImportances = booleanParam(
    name = "calculateFeatureImportances",
    doc = """Compute variable importances for input features (Gedeon method) - can be slow for large networks.""")

  protected val fastMode = booleanParam(
    name = "fastMode",
    doc = """Enable fast mode (minor approximation in back-propagation).""")

  protected val forceLoadBalance = booleanParam(
    name = "forceLoadBalance",
    doc = """Force extra load balancing to increase training speed for small datasets (to keep all cores busy).""")

  protected val replicateTrainingData = booleanParam(
    name = "replicateTrainingData",
    doc = """Replicate the entire training dataset onto every node for faster training on small datasets.""")

  protected val singleNodeMode = booleanParam(
    name = "singleNodeMode",
    doc = """Run on a single node for fine-tuning of model parameters.""")

  protected val shuffleTrainingData = booleanParam(
    name = "shuffleTrainingData",
    doc = """Enable shuffling of training data (recommended if training data is replicated and train_samples_per_iteration is close to #nodes x #rows, of if using balance_classes).""")

  protected val missingValuesHandling = nullableStringParam(
    name = "missingValuesHandling",
    doc = """Handling of missing values. Either MeanImputation or Skip. Possible values are ``"MeanImputation"``, ``"Skip"``.""")

  protected val sparse = booleanParam(
    name = "sparse",
    doc = """Sparse data handling (more efficient for data with lots of 0 values).""")

  protected val averageActivation = doubleParam(
    name = "averageActivation",
    doc = """Average activation for sparse auto-encoder. #Experimental.""")

  protected val sparsityBeta = doubleParam(
    name = "sparsityBeta",
    doc = """Sparsity regularization. #Experimental.""")

  protected val maxCategoricalFeatures = intParam(
    name = "maxCategoricalFeatures",
    doc = """Max. number of categorical features, enforced via hashing. #Experimental.""")

  protected val reproducible = booleanParam(
    name = "reproducible",
    doc = """Force reproducibility on small data (will be slow - only uses 1 thread).""")

  protected val exportWeightsAndBiases = booleanParam(
    name = "exportWeightsAndBiases",
    doc = """Whether to export Neural Network weights and biases to H2O Frames.""")

  protected val miniBatchSize = intParam(
    name = "miniBatchSize",
    doc = """Mini-batch size (smaller leads to better fit, larger can speed up and generalize better).""")

  protected val elasticAveraging = booleanParam(
    name = "elasticAveraging",
    doc = """Elastic averaging between compute nodes can improve distributed model convergence. #Experimental.""")

  protected val elasticAveragingMovingRate = doubleParam(
    name = "elasticAveragingMovingRate",
    doc = """Elastic averaging moving rate (only if elastic averaging is enabled).""")

  protected val elasticAveragingRegularization = doubleParam(
    name = "elasticAveragingRegularization",
    doc = """Elastic averaging regularization strength (only if elastic averaging is enabled).""")

  protected val nfolds = intParam(
    name = "nfolds",
    doc = """Number of folds for K-fold cross-validation (0 to disable or >= 2).""")

  protected val keepCrossValidationModels = booleanParam(
    name = "keepCrossValidationModels",
    doc = """Whether to keep the cross-validation models.""")

  protected val keepCrossValidationPredictions = booleanParam(
    name = "keepCrossValidationPredictions",
    doc = """Whether to keep the predictions of the cross-validation models.""")

  protected val keepCrossValidationFoldAssignment = booleanParam(
    name = "keepCrossValidationFoldAssignment",
    doc = """Whether to keep the cross-validation fold assignment.""")

  protected val distribution = nullableStringParam(
    name = "distribution",
    doc = """Distribution function. Possible values are ``"AUTO"``, ``"bernoulli"``, ``"quasibinomial"``, ``"modified_huber"``, ``"multinomial"``, ``"ordinal"``, ``"gaussian"``, ``"poisson"``, ``"gamma"``, ``"tweedie"``, ``"huber"``, ``"laplace"``, ``"quantile"``, ``"fractionalbinomial"``, ``"negativebinomial"``, ``"custom"``.""")

  protected val tweediePower = doubleParam(
    name = "tweediePower",
    doc = """Tweedie power for Tweedie regression, must be between 1 and 2.""")

  protected val quantileAlpha = doubleParam(
    name = "quantileAlpha",
    doc = """Desired quantile for Quantile regression, must be between 0 and 1.""")

  protected val huberAlpha = doubleParam(
    name = "huberAlpha",
    doc = """Desired quantile for Huber/M-regression (threshold between quadratic and linear loss, must be between 0 and 1).""")

  protected val weightCol = nullableStringParam(
    name = "weightCol",
    doc = """Column with observation weights. Giving some observation a weight of zero is equivalent to excluding it from the dataset; giving an observation a relative weight of 2 is equivalent to repeating that row twice. Negative weights are not allowed. Note: Weights are per-row observation weights and do not increase the size of the data frame. This is typically the number of times a row is repeated, but non-integer values are supported as well. During training, rows with higher weights matter more, due to the larger loss function pre-factor.""")

  protected val foldCol = nullableStringParam(
    name = "foldCol",
    doc = """Column with cross-validation fold index assignment per observation.""")

  protected val foldAssignment = nullableStringParam(
    name = "foldAssignment",
    doc = """Cross-validation fold assignment scheme, if fold_column is not specified. The 'Stratified' option will stratify the folds based on the response variable, for classification problems. Possible values are ``"AUTO"``, ``"Random"``, ``"Modulo"``, ``"Stratified"``.""")

  protected val categoricalEncoding = nullableStringParam(
    name = "categoricalEncoding",
    doc = """Encoding scheme for categorical features. Possible values are ``"AUTO"``, ``"OneHotInternal"``, ``"OneHotExplicit"``, ``"Enum"``, ``"Binary"``, ``"Eigen"``, ``"LabelEncoder"``, ``"SortByResponse"``, ``"EnumLimited"``.""")

  protected val ignoreConstCols = booleanParam(
    name = "ignoreConstCols",
    doc = """Ignore constant columns.""")

  protected val scoreEachIteration = booleanParam(
    name = "scoreEachIteration",
    doc = """Whether to score during each iteration of model training.""")

  protected val stoppingRounds = intParam(
    name = "stoppingRounds",
    doc = """Early stopping based on convergence of stopping_metric. Stop if simple moving average of length k of the stopping_metric does not improve for k:=stopping_rounds scoring events (0 to disable).""")

  protected val maxRuntimeSecs = doubleParam(
    name = "maxRuntimeSecs",
    doc = """Maximum allowed runtime in seconds for model training. Use 0 to disable.""")

  protected val stoppingMetric = nullableStringParam(
    name = "stoppingMetric",
    doc = """Metric to use for early stopping (AUTO: logloss for classification, deviance for regression and anonomaly_score for Isolation Forest). Note that custom and custom_increasing can only be used in GBM and DRF with the Python client. Possible values are ``"AUTO"``, ``"deviance"``, ``"logloss"``, ``"MSE"``, ``"RMSE"``, ``"MAE"``, ``"RMSLE"``, ``"AUC"``, ``"AUCPR"``, ``"lift_top_group"``, ``"misclassification"``, ``"mean_per_class_error"``, ``"anomaly_score"``, ``"custom"``, ``"custom_increasing"``.""")

  protected val stoppingTolerance = doubleParam(
    name = "stoppingTolerance",
    doc = """Relative tolerance for metric-based stopping criterion (stop if relative improvement is not at least this much).""")

  protected val exportCheckpointsDir = nullableStringParam(
    name = "exportCheckpointsDir",
    doc = """Automatically export generated models to this directory.""")

  protected val aucType = nullableStringParam(
    name = "aucType",
    doc = """Set default multinomial AUC type. Possible values are ``"AUTO"``, ``"NONE"``, ``"MACRO_OVR"``, ``"WEIGHTED_OVR"``, ``"MACRO_OVO"``, ``"WEIGHTED_OVO"``.""")

  //
  // Getters
  //
  def getInputCols(): Array[String] = $(inputCols)

  def getOutputCol(): String = $(outputCol)

  def getBalanceClasses(): Boolean = $(balanceClasses)

  def getClassSamplingFactors(): Array[Float] = $(classSamplingFactors)

  def getMaxAfterBalanceSize(): Float = $(maxAfterBalanceSize)

  def getActivation(): String = $(activation)

  def getHidden(): Array[Int] = $(hidden)

  def getEpochs(): Double = $(epochs)

  def getTrainSamplesPerIteration(): Long = $(trainSamplesPerIteration)

  def getTargetRatioCommToComp(): Double = $(targetRatioCommToComp)

  def getSeed(): Long = $(seed)

  def getAdaptiveRate(): Boolean = $(adaptiveRate)

  def getRho(): Double = $(rho)

  def getEpsilon(): Double = $(epsilon)

  def getRate(): Double = $(rate)

  def getRateAnnealing(): Double = $(rateAnnealing)

  def getRateDecay(): Double = $(rateDecay)

  def getMomentumStart(): Double = $(momentumStart)

  def getMomentumRamp(): Double = $(momentumRamp)

  def getMomentumStable(): Double = $(momentumStable)

  def getNesterovAcceleratedGradient(): Boolean = $(nesterovAcceleratedGradient)

  def getInputDropoutRatio(): Double = $(inputDropoutRatio)

  def getHiddenDropoutRatios(): Array[Double] = $(hiddenDropoutRatios)

  def getL1(): Double = $(l1)

  def getL2(): Double = $(l2)

  def getMaxW2(): Float = $(maxW2)

  def getInitialWeightDistribution(): String = $(initialWeightDistribution)

  def getInitialWeightScale(): Double = $(initialWeightScale)

  def getLoss(): String = $(loss)

  def getScoreInterval(): Double = $(scoreInterval)

  def getScoreTrainingSamples(): Long = $(scoreTrainingSamples)

  def getScoreValidationSamples(): Long = $(scoreValidationSamples)

  def getScoreDutyCycle(): Double = $(scoreDutyCycle)

  def getClassificationStop(): Double = $(classificationStop)

  def getRegressionStop(): Double = $(regressionStop)

  def getQuietMode(): Boolean = $(quietMode)

  def getScoreValidationSampling(): String = $(scoreValidationSampling)

  def getOverwriteWithBestModel(): Boolean = $(overwriteWithBestModel)

  def getUseAllFactorLevels(): Boolean = $(useAllFactorLevels)

  def getStandardize(): Boolean = $(standardize)

  def getDiagnostics(): Boolean = $(diagnostics)

  def getCalculateFeatureImportances(): Boolean = $(calculateFeatureImportances)

  def getFastMode(): Boolean = $(fastMode)

  def getForceLoadBalance(): Boolean = $(forceLoadBalance)

  def getReplicateTrainingData(): Boolean = $(replicateTrainingData)

  def getSingleNodeMode(): Boolean = $(singleNodeMode)

  def getShuffleTrainingData(): Boolean = $(shuffleTrainingData)

  def getMissingValuesHandling(): String = $(missingValuesHandling)

  def getSparse(): Boolean = $(sparse)

  def getAverageActivation(): Double = $(averageActivation)

  def getSparsityBeta(): Double = $(sparsityBeta)

  def getMaxCategoricalFeatures(): Int = $(maxCategoricalFeatures)

  def getReproducible(): Boolean = $(reproducible)

  def getExportWeightsAndBiases(): Boolean = $(exportWeightsAndBiases)

  def getMiniBatchSize(): Int = $(miniBatchSize)

  def getElasticAveraging(): Boolean = $(elasticAveraging)

  def getElasticAveragingMovingRate(): Double = $(elasticAveragingMovingRate)

  def getElasticAveragingRegularization(): Double = $(elasticAveragingRegularization)

  def getNfolds(): Int = $(nfolds)

  def getKeepCrossValidationModels(): Boolean = $(keepCrossValidationModels)

  def getKeepCrossValidationPredictions(): Boolean = $(keepCrossValidationPredictions)

  def getKeepCrossValidationFoldAssignment(): Boolean = $(keepCrossValidationFoldAssignment)

  def getDistribution(): String = $(distribution)

  def getTweediePower(): Double = $(tweediePower)

  def getQuantileAlpha(): Double = $(quantileAlpha)

  def getHuberAlpha(): Double = $(huberAlpha)

  def getWeightCol(): String = $(weightCol)

  def getFoldCol(): String = $(foldCol)

  def getFoldAssignment(): String = $(foldAssignment)

  def getCategoricalEncoding(): String = $(categoricalEncoding)

  def getIgnoreConstCols(): Boolean = $(ignoreConstCols)

  def getScoreEachIteration(): Boolean = $(scoreEachIteration)

  def getStoppingRounds(): Int = $(stoppingRounds)

  def getMaxRuntimeSecs(): Double = $(maxRuntimeSecs)

  def getStoppingMetric(): String = $(stoppingMetric)

  def getStoppingTolerance(): Double = $(stoppingTolerance)

  def getExportCheckpointsDir(): String = $(exportCheckpointsDir)

  def getAucType(): String = $(aucType)

  override private[sparkling] def setSpecificParams(h2oMojo: MojoModel): Unit = {
    super.setSpecificParams(h2oMojo)
    try {
      val h2oParameters = h2oMojo._modelAttributes.getModelParameters()
      val h2oParametersMap = h2oParameters.map(i => i.name -> i.actual_value).toMap

      try {
        h2oParametersMap.get("balance_classes").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("balanceClasses", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'balanceClasses' parameter.", e)
      }

      try {
        h2oParametersMap.get("class_sampling_factors").foreach { value =>
          val convertedValue = if (value.isInstanceOf[Array[Double]]) {
            value.asInstanceOf[Array[Double]].map(_.toFloat)
          } else {
            value
          }
          set("classSamplingFactors", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'classSamplingFactors' parameter.", e)
      }

      try {
        h2oParametersMap.get("max_after_balance_size").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Float].floatValue()
          set("maxAfterBalanceSize", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'maxAfterBalanceSize' parameter.", e)
      }

      try {
        h2oParametersMap.get("activation").foreach { value =>
          val convertedValue = value
          set("activation", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'activation' parameter.", e)
      }

      try {
        h2oParametersMap.get("hidden").foreach { value =>
          val convertedValue = value
          set("hidden", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'hidden' parameter.", e)
      }

      try {
        h2oParametersMap.get("epochs").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("epochs", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'epochs' parameter.", e)
      }

      try {
        h2oParametersMap.get("train_samples_per_iteration").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Long].longValue()
          set("trainSamplesPerIteration", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'trainSamplesPerIteration' parameter.", e)
      }

      try {
        h2oParametersMap.get("target_ratio_comm_to_comp").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("targetRatioCommToComp", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'targetRatioCommToComp' parameter.", e)
      }

      try {
        h2oParametersMap.get("seed").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Long].longValue()
          set("seed", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'seed' parameter.", e)
      }

      try {
        h2oParametersMap.get("adaptive_rate").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("adaptiveRate", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'adaptiveRate' parameter.", e)
      }

      try {
        h2oParametersMap.get("rho").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("rho", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'rho' parameter.", e)
      }

      try {
        h2oParametersMap.get("epsilon").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("epsilon", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'epsilon' parameter.", e)
      }

      try {
        h2oParametersMap.get("rate").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("rate", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'rate' parameter.", e)
      }

      try {
        h2oParametersMap.get("rate_annealing").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("rateAnnealing", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'rateAnnealing' parameter.", e)
      }

      try {
        h2oParametersMap.get("rate_decay").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("rateDecay", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'rateDecay' parameter.", e)
      }

      try {
        h2oParametersMap.get("momentum_start").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("momentumStart", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'momentumStart' parameter.", e)
      }

      try {
        h2oParametersMap.get("momentum_ramp").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("momentumRamp", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'momentumRamp' parameter.", e)
      }

      try {
        h2oParametersMap.get("momentum_stable").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("momentumStable", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'momentumStable' parameter.", e)
      }

      try {
        h2oParametersMap.get("nesterov_accelerated_gradient").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("nesterovAcceleratedGradient", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'nesterovAcceleratedGradient' parameter.", e)
      }

      try {
        h2oParametersMap.get("input_dropout_ratio").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("inputDropoutRatio", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'inputDropoutRatio' parameter.", e)
      }

      try {
        h2oParametersMap.get("hidden_dropout_ratios").foreach { value =>
          val convertedValue = value
          set("hiddenDropoutRatios", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'hiddenDropoutRatios' parameter.", e)
      }

      try {
        h2oParametersMap.get("l1").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("l1", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'l1' parameter.", e)
      }

      try {
        h2oParametersMap.get("l2").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("l2", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'l2' parameter.", e)
      }

      try {
        h2oParametersMap.get("max_w2").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Float].floatValue()
          set("maxW2", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'maxW2' parameter.", e)
      }

      try {
        h2oParametersMap.get("initial_weight_distribution").foreach { value =>
          val convertedValue = value
          set("initialWeightDistribution", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'initialWeightDistribution' parameter.", e)
      }

      try {
        h2oParametersMap.get("initial_weight_scale").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("initialWeightScale", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'initialWeightScale' parameter.", e)
      }

      try {
        h2oParametersMap.get("loss").foreach { value =>
          val convertedValue = value
          set("loss", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'loss' parameter.", e)
      }

      try {
        h2oParametersMap.get("score_interval").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("scoreInterval", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'scoreInterval' parameter.", e)
      }

      try {
        h2oParametersMap.get("score_training_samples").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Long].longValue()
          set("scoreTrainingSamples", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'scoreTrainingSamples' parameter.", e)
      }

      try {
        h2oParametersMap.get("score_validation_samples").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Long].longValue()
          set("scoreValidationSamples", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'scoreValidationSamples' parameter.", e)
      }

      try {
        h2oParametersMap.get("score_duty_cycle").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("scoreDutyCycle", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'scoreDutyCycle' parameter.", e)
      }

      try {
        h2oParametersMap.get("classification_stop").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("classificationStop", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'classificationStop' parameter.", e)
      }

      try {
        h2oParametersMap.get("regression_stop").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("regressionStop", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'regressionStop' parameter.", e)
      }

      try {
        h2oParametersMap.get("quiet_mode").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("quietMode", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'quietMode' parameter.", e)
      }

      try {
        h2oParametersMap.get("score_validation_sampling").foreach { value =>
          val convertedValue = value
          set("scoreValidationSampling", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'scoreValidationSampling' parameter.", e)
      }

      try {
        h2oParametersMap.get("overwrite_with_best_model").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("overwriteWithBestModel", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'overwriteWithBestModel' parameter.", e)
      }

      try {
        h2oParametersMap.get("use_all_factor_levels").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("useAllFactorLevels", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'useAllFactorLevels' parameter.", e)
      }

      try {
        h2oParametersMap.get("standardize").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("standardize", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'standardize' parameter.", e)
      }

      try {
        h2oParametersMap.get("diagnostics").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("diagnostics", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'diagnostics' parameter.", e)
      }

      try {
        h2oParametersMap.get("variable_importances").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("calculateFeatureImportances", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'calculateFeatureImportances' parameter.", e)
      }

      try {
        h2oParametersMap.get("fast_mode").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("fastMode", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'fastMode' parameter.", e)
      }

      try {
        h2oParametersMap.get("force_load_balance").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("forceLoadBalance", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'forceLoadBalance' parameter.", e)
      }

      try {
        h2oParametersMap.get("replicate_training_data").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("replicateTrainingData", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'replicateTrainingData' parameter.", e)
      }

      try {
        h2oParametersMap.get("single_node_mode").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("singleNodeMode", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'singleNodeMode' parameter.", e)
      }

      try {
        h2oParametersMap.get("shuffle_training_data").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("shuffleTrainingData", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'shuffleTrainingData' parameter.", e)
      }

      try {
        h2oParametersMap.get("missing_values_handling").foreach { value =>
          val convertedValue = value
          set("missingValuesHandling", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'missingValuesHandling' parameter.", e)
      }

      try {
        h2oParametersMap.get("sparse").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("sparse", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'sparse' parameter.", e)
      }

      try {
        h2oParametersMap.get("average_activation").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("averageActivation", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'averageActivation' parameter.", e)
      }

      try {
        h2oParametersMap.get("sparsity_beta").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("sparsityBeta", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'sparsityBeta' parameter.", e)
      }

      try {
        h2oParametersMap.get("max_categorical_features").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Integer].intValue()
          set("maxCategoricalFeatures", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'maxCategoricalFeatures' parameter.", e)
      }

      try {
        h2oParametersMap.get("reproducible").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("reproducible", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'reproducible' parameter.", e)
      }

      try {
        h2oParametersMap.get("export_weights_and_biases").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("exportWeightsAndBiases", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'exportWeightsAndBiases' parameter.", e)
      }

      try {
        h2oParametersMap.get("mini_batch_size").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Integer].intValue()
          set("miniBatchSize", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'miniBatchSize' parameter.", e)
      }

      try {
        h2oParametersMap.get("elastic_averaging").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("elasticAveraging", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'elasticAveraging' parameter.", e)
      }

      try {
        h2oParametersMap.get("elastic_averaging_moving_rate").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("elasticAveragingMovingRate", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'elasticAveragingMovingRate' parameter.", e)
      }

      try {
        h2oParametersMap.get("elastic_averaging_regularization").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("elasticAveragingRegularization", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'elasticAveragingRegularization' parameter.", e)
      }

      try {
        h2oParametersMap.get("nfolds").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Integer].intValue()
          set("nfolds", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'nfolds' parameter.", e)
      }

      try {
        h2oParametersMap.get("keep_cross_validation_models").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("keepCrossValidationModels", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'keepCrossValidationModels' parameter.", e)
      }

      try {
        h2oParametersMap.get("keep_cross_validation_predictions").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("keepCrossValidationPredictions", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'keepCrossValidationPredictions' parameter.", e)
      }

      try {
        h2oParametersMap.get("keep_cross_validation_fold_assignment").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("keepCrossValidationFoldAssignment", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'keepCrossValidationFoldAssignment' parameter.", e)
      }

      try {
        h2oParametersMap.get("distribution").foreach { value =>
          val convertedValue = value
          set("distribution", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'distribution' parameter.", e)
      }

      try {
        h2oParametersMap.get("tweedie_power").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("tweediePower", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'tweediePower' parameter.", e)
      }

      try {
        h2oParametersMap.get("quantile_alpha").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("quantileAlpha", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'quantileAlpha' parameter.", e)
      }

      try {
        h2oParametersMap.get("huber_alpha").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("huberAlpha", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'huberAlpha' parameter.", e)
      }

      try {
        h2oParametersMap.get("weights_column").foreach { value =>
          val convertedValue = if (value == null) null else value.asInstanceOf[hex.genmodel.attributes.parameters.ColumnSpecifier].getColumnName()
          set("weightCol", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'weightCol' parameter.", e)
      }

      try {
        h2oParametersMap.get("fold_column").foreach { value =>
          val convertedValue = if (value == null) null else value.asInstanceOf[hex.genmodel.attributes.parameters.ColumnSpecifier].getColumnName()
          set("foldCol", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'foldCol' parameter.", e)
      }

      try {
        h2oParametersMap.get("fold_assignment").foreach { value =>
          val convertedValue = value
          set("foldAssignment", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'foldAssignment' parameter.", e)
      }

      try {
        h2oParametersMap.get("categorical_encoding").foreach { value =>
          val convertedValue = value
          set("categoricalEncoding", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'categoricalEncoding' parameter.", e)
      }

      try {
        h2oParametersMap.get("ignore_const_cols").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("ignoreConstCols", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'ignoreConstCols' parameter.", e)
      }

      try {
        h2oParametersMap.get("score_each_iteration").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Boolean].booleanValue()
          set("scoreEachIteration", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'scoreEachIteration' parameter.", e)
      }

      try {
        h2oParametersMap.get("stopping_rounds").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Integer].intValue()
          set("stoppingRounds", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'stoppingRounds' parameter.", e)
      }

      try {
        h2oParametersMap.get("max_runtime_secs").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("maxRuntimeSecs", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'maxRuntimeSecs' parameter.", e)
      }

      try {
        h2oParametersMap.get("stopping_metric").foreach { value =>
          val convertedValue = value
          set("stoppingMetric", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'stoppingMetric' parameter.", e)
      }

      try {
        h2oParametersMap.get("stopping_tolerance").foreach { value =>
          val convertedValue = value.asInstanceOf[java.lang.Double].doubleValue()
          set("stoppingTolerance", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'stoppingTolerance' parameter.", e)
      }

      try {
        h2oParametersMap.get("export_checkpoints_dir").foreach { value =>
          val convertedValue = value
          set("exportCheckpointsDir", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'exportCheckpointsDir' parameter.", e)
      }

      try {
        h2oParametersMap.get("auc_type").foreach { value =>
          val convertedValue = value
          set("aucType", convertedValue)
        }
      } catch {
        case e: Throwable => logError("An error occurred during setting up the 'aucType' parameter.", e)
      }
    } catch {
      case e: Throwable => logError("An error occurred during a try to access H2O MOJO parameters.", e)
    }
  }
}
    