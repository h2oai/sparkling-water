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

import java.io._
import java.util.Date

import ai.h2o.automl.{AutoML, AutoMLBuildSpec}
import hex.ScoreKeeper
import org.apache.hadoop.fs.Path
import org.apache.spark.annotation.{DeveloperApi, Since}
import org.apache.spark.h2o._
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.h2o.models.H2OMOJOModel
import org.apache.spark.ml.h2o.param.NullableStringParam
import org.apache.spark.ml.param._
import org.apache.spark.ml.util._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Dataset, SQLContext}
import org.json4s.JsonAST.JArray
import org.json4s.jackson.JsonMethods.{compact, parse, render}
import org.json4s.{JNull, JString, JValue}
import water.Key
import water.support.{H2OFrameSupport, ModelSerializationSupport}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

/**
  * H2O AutoML pipeline step
  */
class H2OAutoML(val automlBuildSpec: Option[AutoMLBuildSpec], override val uid: String)
               (implicit hc: H2OContext, sqlContext: SQLContext)
  extends Estimator[H2OMOJOModel] with MLWritable with H2OAutoMLParams {

  def this()(implicit hc: H2OContext, sqlContext: SQLContext) = this(None, Identifiable.randomUID("automl"))

  def this(uid: String, hc: H2OContext, sqlContext: SQLContext) = this(None, uid)(hc, sqlContext)

  override def fit(dataset: Dataset[_]): H2OMOJOModel = {
    val spec = automlBuildSpec.getOrElse(new AutoMLBuildSpec)

    // override the buildSpec with the configuration specified directly on the estimator

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

    spec.input_spec.response_column = getPredictionsCol()
    spec.input_spec.fold_column = getFoldColumn()
    spec.input_spec.weights_column = getWeightsColumn()
    spec.input_spec.ignored_columns = getIgnoredColumns()
    spec.feature_engineering.try_mutations = getTryMutations()
    spec.build_models.exclude_algos = getExcludeAlgos()
    spec.build_control.project_name = getProjectName()
    spec.build_control.loss = getLoss()
    spec.build_control.stopping_criteria.set_seed(getSeed())
    spec.build_control.stopping_criteria.set_max_runtime_secs(getMaxRuntimeSecs())
    spec.build_control.stopping_criteria.set_stopping_rounds(getStoppingRounds())
    spec.build_control.stopping_criteria.set_stopping_tolerance(getStoppingTolerance())
    spec.build_control.stopping_criteria.set_stopping_metric(getStoppingMetric())
    spec.build_control.nfolds = getNfolds()

    water.DKV.put(trainFrame)
    val aml = new AutoML(Key.make(uid), new Date(), spec)
    AutoML.startAutoML(aml)
    // Block until AutoML finishes
    aml.get()
    val model = trainModel(aml)
    model.setConvertUnknownCategoricalLevelsToNa(true)
    model
  }


  def trainModel(aml: AutoML) = {
    new H2OMOJOModel(ModelSerializationSupport.getMojoData(aml.leader()))
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    schema
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  @Since("1.6.0")
  override def write: MLWriter = new H2OAutoMLWriter(this)

  def defaultFileName: String = H2OAutoML.defaultFileName
}


object H2OAutoML extends MLReadable[H2OAutoML] {

  private final val defaultFileName = "automl_params"

  @Since("1.6.0")
  override def read: MLReader[H2OAutoML] = H2OAutoMLReader.create[H2OAutoML](defaultFileName)

  @Since("1.6.0")
  override def load(path: String): H2OAutoML = super.load(path)
}

// FIXME: H2O Params are iced objects!
private[algos] class H2OAutoMLWriter(instance: H2OAutoML) extends MLWriter {

  @Since("1.6.0") override protected def saveImpl(path: String): Unit = {
    val hadoopConf = sc.hadoopConfiguration
    DefaultParamsWriter.saveMetadata(instance, path, sc)
    val outputPath = new Path(path, instance.defaultFileName)
    val fs = outputPath.getFileSystem(hadoopConf)
    val qualifiedOutputPath = outputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val out = fs.create(qualifiedOutputPath)
    val oos = new ObjectOutputStream(out)
    oos.writeObject(instance.automlBuildSpec.orNull)
    out.close()
    logInfo(s"Saved to: $qualifiedOutputPath")
  }
}

private[algos] class H2OAutoMLReader[A <: H2OAutoML : ClassTag](val defaultFileName: String) extends MLReader[A] {

  override def load(path: String): A = {
    val metadata = DefaultParamsReader.loadMetadata(path, sc)

    val inputPath = new Path(path, defaultFileName)
    val fs = inputPath.getFileSystem(sc.hadoopConfiguration)
    val qualifiedInputPath = inputPath.makeQualified(fs.getUri, fs.getWorkingDirectory)
    val ois = new ObjectInputStream(fs.open(qualifiedInputPath))

    val buildSpec = ois.readObject().asInstanceOf[AutoMLBuildSpec]
    implicit val h2oContext: H2OContext = H2OContext.ensure("H2OContext has to be started in order to use H2O pipelines elements.")
    val algo = make[A](Option(buildSpec), metadata.uid, h2oContext, sqlContext)
    DefaultParamsReader.getAndSetParams(algo, metadata)
    algo
  }

  private def make[CT: ClassTag]
  (autoMLBuildSpec: Option[AutoMLBuildSpec], uid: String, h2oContext: H2OContext, sqlContext: SQLContext): CT = {
    val aClass = implicitly[ClassTag[CT]].runtimeClass
    val ctor = aClass.getConstructor(classOf[Option[AutoMLBuildSpec]], classOf[String], classOf[H2OContext], classOf[SQLContext])
    ctor.newInstance(autoMLBuildSpec, uid, h2oContext, sqlContext).asInstanceOf[CT]
  }
}


object H2OAutoMLReader {
  def create[A <: H2OAutoML : ClassTag](defaultFileName: String) = new H2OAutoMLReader[A](defaultFileName)
}

trait H2OAutoMLParams extends Params {

  //
  // Param definitions
  //
  private final val predictionCol = new NullableStringParam(this, "predictionCol", "Prediction column name")
  private final val allStringColumnsToCategorical = new BooleanParam(this, "allStringColumnsToCategorical", "Transform all strings columns to categorical")
  private final val columnsToCategorical = new StringArrayParam(this, "columnsToCategorical", "List of columns to convert to categoricals before modelling")
  private final val ratio = new DoubleParam(this, "ratio", "Determines in which ratios split the dataset")
  private final val foldColumn = new NullableStringParam(this, "foldColumn", "Fold column name")
  private final val weightsColumn = new NullableStringParam(this, "weightsColumn", "Weights column name")
  private final val ignoredColumns = new StringArrayParam(this, "ignoredColumns", "Ignored columns names")
  private final val tryMutations = new BooleanParam(this, "tryMutations", "Whether to use mutations as part of the feature engineering")
  private final val excludeAlgos = new H2OAutoMLAlgosParam(this, "excludeAlgos", "Algorithms to exclude when using automl")
  private final val projectName = new NullableStringParam(this, "projectName", "Identifier for models that should be grouped together in the leaderboard" +
    " (e.g., airlines and iris)")
  private final val loss = new NullableStringParam(this, "loss", "loss")
  private final val maxRuntimeSecs = new DoubleParam(this, "maxRuntimeSecs", "Maximum time in seconds for automl to be running")
  private final val stoppingRounds = new IntParam(this, "stoppingRounds", "Stopping rounds")
  private final val stoppingTolerance = new DoubleParam(this, "stoppingTolerance", "Stopping tolerance")
  private final val stoppingMetric = new H2OAutoMLStoppingMetricParam(this, "stoppingMetric", "Stopping metric")
  private final val nfolds = new IntParam(this, "nfolds", "Cross-validation fold construction")
  private final val convertUnknownCategoricalLevelsToNa = new BooleanParam(this, "convertUnknownCategoricalLevelsToNa", "Convert unknown" +
    " categorical levels to NA during predictions")
  private final val seed = new IntParam(this, "seed", "seed")

  //
  // Default values
  //
  setDefault(
    predictionCol -> "predictionCol",
    allStringColumnsToCategorical -> true,
    columnsToCategorical -> Array.empty[String],
    ratio -> 1.0, // 1.0 means use whole frame as training frame,
    foldColumn -> null,
    weightsColumn -> null,
    ignoredColumns -> Array.empty[String],
    tryMutations -> true,
    excludeAlgos -> null,
    projectName -> null, // will be automatically generated
    loss -> "AUTO",
    maxRuntimeSecs -> 3600,
    stoppingRounds -> 3,
    stoppingTolerance -> 0.001,
    stoppingMetric -> ScoreKeeper.StoppingMetric.AUTO,
    nfolds -> 5,
    convertUnknownCategoricalLevelsToNa -> false,
    seed -> -1 // true random
  )

  //
  // Getters
  //
  /** @group getParam */
  def getPredictionsCol() = $(predictionCol)

  /** @group getParam */
  def getAllStringColumnsToCategorical() = $(allStringColumnsToCategorical)

  /** @group getParam */
  def getColumnsToCategorical() = $(columnsToCategorical)

  /** @group getParam */
  def getRatio() = $(ratio)

  /** @group getParam */
  def getFoldColumn() = $(foldColumn)

  /** @group getParam */
  def getWeightsColumn() = $(weightsColumn)

  /** @group getParam */
  def getIgnoredColumns() = $(ignoredColumns)

  /** @group getParam */
  def getTryMutations() = $(tryMutations)

  /** @group getParam */
  def getExcludeAlgos() = $(excludeAlgos)

  /** @group getParam */
  def getProjectName() = $(projectName)

  /** @group getParam */
  def getLoss() = $(loss)

  /** @group getParam */
  def getMaxRuntimeSecs() = $(maxRuntimeSecs)

  /** @group getParam */
  def getStoppingRounds() = $(stoppingRounds)

  /** @group getParam */
  def getStoppingTolerance() = $(stoppingTolerance)

  /** @group getParam */
  def getStoppingMetric() = $(stoppingMetric)

  /** @group getParam */
  def getNfolds() = $(nfolds)

  /** @group getParam */
  def getConvertUnknownCategoricalLevelsToNa() = $(convertUnknownCategoricalLevelsToNa)
  /** @group getParam */
  def getSeed() = $(seed)

  //
  // Setters
  //
  /** @group setParam */
  def setPredictionsCol(value: String): this.type = set(predictionCol, value)

  /** @group setParam */
  def setAllStringColumnsToCategorical(value: Boolean): this.type = set(allStringColumnsToCategorical, value)

  /** @group setParam */
  def setColumnsToCategorical(first: String, others: String*): this.type  = set(columnsToCategorical, Array(first) ++ others)

  /** @group setParam */
  def setColumnsToCategorical(columns: Array[String]): this.type  = set(columnsToCategorical, columns)

  /** @group setParam */
  def setRatio(value: Double): this.type = set(ratio, value)

  /** @group setParam */
  def setFoldColumn(value: String): this.type = set(foldColumn, value)

  /** @group setParam */
  def setWeightsColumn(value: String): this.type = set(weightsColumn, value)

  /** @group setParam */
  def setIgnoredColumns(value: Array[String]): this.type = set(ignoredColumns, value)

  /** @group setParam */
  def setTryMutations(value: Boolean): this.type = set(tryMutations, value)

  /** @group setParam */
  def setExcludeAlgos(value: Array[AutoML.algo]): this.type = set(excludeAlgos, value)

  /** @group setParam */
  def setProjectName(value: String): this.type = set(projectName, value)

  /** @group setParam */
  def setLoss(value: String): this.type = set(loss, value)

  /** @group setParam */
  def setMaxRuntimeSecs(value: Double): this.type = set(maxRuntimeSecs, value)

  /** @group setParam */
  def setStoppingRounds(value: Int): this.type = set(stoppingRounds, value)

  /** @group setParam */
  def setStoppingTolerance(value: Double): this.type = set(stoppingTolerance, value)

  /** @group setParam */
  def setStoppingMetric(value: ScoreKeeper.StoppingMetric): this.type = set(stoppingMetric, value)

  /** @group setParam */
  def setNfolds(value: Int): this.type = set(nfolds, value)

  /** @group setParam */
  def setConvertUnknownCategoricalLevelsToNa(value: Boolean): this.type = set(convertUnknownCategoricalLevelsToNa, value)

  /** @group setParam */
  def setSeed(value: Int): this.type = set(seed, value)

}

class H2OAutoMLAlgosParam private(parent: Params, name: String, doc: String, isValid: Array[AutoML.algo] => Boolean)
  extends Param[Array[AutoML.algo]](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)

  /** Creates a param pair with a `java.util.List` of values (for Java and Python). */
  def w(value: java.util.List[AutoML.algo]): ParamPair[Array[AutoML.algo]] = w(value.asScala.toArray)

  override def jsonEncode(value: Array[AutoML.algo]): String = {
    val encoded: JValue = if (value == null) {
      JNull
    } else {
      JArray(value.map(algo => JString(algo.toString)).toList)
    }
    compact(render(encoded))
  }


  override def jsonDecode(json: String): Array[AutoML.algo] = {
    parse(json) match {
      case JArray(values) =>
        values.map {
          case JString(x) =>
            AutoML.algo.valueOf(x)
          case _ =>
            throw new IllegalArgumentException(s"Cannot decode $json to AutoML.algo.")
        }.toArray
      case JNull =>
        null
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $json to Array[AutoML.algo].")
    }
  }
}

class H2OAutoMLStoppingMetricParam private(parent: Params, name: String, doc: String, isValid: ScoreKeeper.StoppingMetric => Boolean)
  extends Param[ScoreKeeper.StoppingMetric](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: ScoreKeeper.StoppingMetric): ParamPair[ScoreKeeper.StoppingMetric] = super.w(value)

  override def jsonEncode(value: ScoreKeeper.StoppingMetric): String = {
    val encoded: JValue = if (value == null) {
      JNull
    } else {
      JString(value.toString)
    }
    compact(render(encoded))
  }

  override def jsonDecode(json: String): ScoreKeeper.StoppingMetric = {
    val parsed = parse(json)
    parsed match {
      case JString(x) =>
        ScoreKeeper.StoppingMetric.valueOf(x)
      case JNull =>
        null
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $parsed to ScoreKeeper.StoppingMetric.")
    }

  }
}

