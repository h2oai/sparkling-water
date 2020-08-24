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

import ai.h2o.sparkling.ml.params.AlgoParam
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import hex.Model
import hex.tree.gbm.GBMModel.GBMParameters
import org.apache.spark.sql.functions._
import org.apache.spark.ml.param.{ParamMap, Params}
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.SparkSession
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class H2OGridSearchTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val dataset = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/prostate/prostate.csv"))

  test("H2O Grid Search GLM Pipeline") {
    val glm = new H2OGLM().setLabelCol("AGE")
    val hyperParams: mutable.HashMap[String, Array[AnyRef]] = mutable.HashMap()

    testGridSearch(glm, hyperParams)
  }

  test("H2O Grid Search GBM Pipeline") {
    val gbm = new H2OGBM().setLabelCol("AGE")
    val hyperParams: mutable.HashMap[String, Array[AnyRef]] = mutable.HashMap()
    hyperParams += ("ntrees" -> Array(1, 10, 30).map(_.asInstanceOf[AnyRef]), "seed" -> Array(1, 2).map(
      _.asInstanceOf[AnyRef]))

    testGridSearch(gbm, hyperParams)
  }

  test("H2O Grid Search DeepLearning Pipeline") {
    val deeplearning = new H2ODeepLearning().setLabelCol("AGE")
    val hyperParams: mutable.HashMap[String, Array[AnyRef]] = mutable.HashMap()

    testGridSearch(deeplearning, hyperParams)
  }

  test("The getGridModelsParams() is able return nested hyper-parameter values") {
    val deeplearning = new H2ODeepLearning()
      .setSeed(1)
      .setLabelCol("AGE")
    val hyperParams = Map[String, Array[AnyRef]](
      "hidden" -> Array(Array(10, 20, 30)),
      "epochs" -> Array[AnyRef](new java.lang.Double(3.0)))
    val grid = new H2OGridSearch()
      .setHyperParameters(hyperParams)
      .setAlgo(deeplearning)

    grid.fit(dataset)
    val result = grid.getGridModelsParams().first()

    result.getAs[String]("hidden") shouldEqual "[10, 20, 30]"
    result.getAs[String]("epochs") shouldEqual "3.0"
  }

  test("H2O Grid Search XGBoost Pipeline") {
    val xgboost = new H2OXGBoost().setLabelCol("AGE")
    val hyperParams: mutable.HashMap[String, Array[AnyRef]] = mutable.HashMap()

    testGridSearch(xgboost, hyperParams)
  }

  test("Exception thrown when parameter is not gridable") {
    val drf = new H2ODRF().setLabelCol("AGE")
    val hyperParams: mutable.HashMap[String, Array[AnyRef]] = mutable.HashMap()
    hyperParams += "binomialDoubleTrees" -> Array(true, false).map(_.asInstanceOf[AnyRef])
    val thrown = intercept[IllegalArgumentException] {
      testGridSearch(drf, hyperParams)
    }
    assert(thrown.getMessage == "Parameter 'binomial_double_trees' is not supported to be passed as hyper parameter!")
  }

  test("H2O Grid Search DRF Pipeline") {
    val drf = new H2ODRF().setLabelCol("AGE")
    val hyperParams: mutable.HashMap[String, Array[AnyRef]] = mutable.HashMap()
    hyperParams += "ntrees" -> Array(1, 10, 30).map(_.asInstanceOf[AnyRef])
    hyperParams += "seed" -> Array(1, 2).map(_.asInstanceOf[AnyRef])
    hyperParams += "mtries" -> Array(-1, 5, 10).map(_.asInstanceOf[AnyRef])
    testGridSearch(drf, hyperParams)
  }

  test("H2O Grid Search KMeans Pipeline") {
    val kmeans = new H2OKMeans()
    val hyperParams: mutable.HashMap[String, Array[AnyRef]] = mutable.HashMap()
    hyperParams += "k" -> Array(1, 10, 30).map(_.asInstanceOf[AnyRef])
    testGridSearch(kmeans, hyperParams)
  }

  test("H2O Grid Search GLRM Pipeline") {
    val glrm = new H2OGLRM()
    val hyperParams: mutable.HashMap[String, Array[AnyRef]] = mutable.HashMap()
    hyperParams += "init" -> Array("Random", "PlusPlus").map(_.asInstanceOf[AnyRef])
    hyperParams += "svdMethod" -> Array("GramSVD", "Power", "Randomized").map(_.asInstanceOf[AnyRef])
    testGridSearch(glrm, hyperParams)
  }

  test("H2O Grid Search PCA Pipeline") {
    val pca = new H2OPCA()
    val hyperParams: mutable.HashMap[String, Array[AnyRef]] = mutable.HashMap()
    hyperParams += "k" -> Array(3, 4).map(_.asInstanceOf[AnyRef])
    hyperParams += "transform" -> Array("STANDARDIZE", "NORMALIZE", "DEMEAN").map(_.asInstanceOf[AnyRef])
    testGridSearch(pca, hyperParams)
  }

  test("H2O Grid Search Isolation Forest Pipeline") {
    val validationDataset = spark.read
      .option("header", "true")
      .option("inferSchema", "true")
      .csv(TestUtils.locate("smalldata/prostate/prostate_anomaly_validation.csv"))

    val isolationForest = new H2OIsolationForest()
      .setValidationDataFrame(validationDataset)
      .setValidationLabelCol("isAnomaly")
    val hyperParams: mutable.HashMap[String, Array[AnyRef]] = mutable.HashMap()
    hyperParams += "ntrees" -> Array(10, 20, 30).map(_.asInstanceOf[AnyRef])
    hyperParams += "maxDepth" -> Array(5, 10, 20).map(_.asInstanceOf[AnyRef])

    val stage = new H2OGridSearch()
      .setHyperParameters(hyperParams)
      .setAlgo(isolationForest)

    val pipeline = new Pipeline().setStages(Array(stage))
    pipeline.write.overwrite().save(s"ml/build/grid_isolation_forest_pipeline")
    val loadedPipeline = Pipeline.load(s"ml/build/grid_isolation_forest_pipeline")
    val model = loadedPipeline.fit(dataset)

    model.write.overwrite().save(s"ml/build/grid_isolation_forest_pipeline_model")
    val loadedModel = PipelineModel.load(s"ml/build/grid_isolation_forest_pipeline_model")

    loadedModel.transform(dataset).count()
  }

  private val parentParams = new Params {
    override def copy(extra: ParamMap): Params = throw new UnsupportedOperationException

    override val uid: String = "test_params"
  }

  test("Serialize null AlgoParam") {
    val algoParam = new AlgoParam(parentParams, "algo", "desc")
    val encoded = algoParam.jsonEncode(null)
    assert(encoded == "null")
  }

  test("Deserialize null AlgoParam") {
    val algoParam = new AlgoParam(parentParams, "algo", "desc")
    val decoded = algoParam.jsonDecode("null")
    assert(decoded == null)
  }

  test("Serialize algo param") {

    val algoParam = new AlgoParam(parentParams, "algo", "desc")
    val gbm = new H2OGBM().setLabelCol("AGE")
    val encoded = algoParam.jsonEncode(gbm)
    assert(encoded.contains("\"class\":\"ai.h2o.sparkling.ml.algos.H2OGBM\""))
    assert(encoded.contains("\"labelCol\":\"AGE\""))
  }

  test("Deserialize algo param") {
    val algoParam = new AlgoParam(parentParams, "algo", "desc")
    val gbm = new H2OGBM().setLabelCol("AGE").setColumnsToCategorical(Array("a", "b"))
    val encoded = algoParam.jsonEncode(gbm)

    val algo = algoParam.jsonDecode(encoded).asInstanceOf[H2OSupervisedAlgorithm[GBMParameters]]
    assert(algo.isInstanceOf[H2OGBM])
    assert(algo.getLabelCol() == "AGE")
    val gbmParamMap = gbm.extractParamMap()
    algo.extractParamMap().toSeq.foreach { paramPair =>
      if (paramPair.param.name == "AGE") {
        assert(paramPair.value == gbmParamMap.get(paramPair.param).get)
      }
    }
  }

  test("H2O GridSearch with nfolds") {
    val drf = new H2ODRF()
      .setFeaturesCols(Array("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"))
      .setLabelCol("CAPSULE")
      .setColumnsToCategorical(Array("RACE", "DPROS", "DCAPS", "GLEASON", "CAPSULE"))
      .setSplitRatio(0.8)
      .setNfolds(4)

    val hyperParams = Map("ntrees" -> Array(10, 50).map(_.asInstanceOf[AnyRef]))

    val search = new H2OGridSearch()
      .setHyperParameters(hyperParams)
      .setAlgo(drf)
      .setStrategy("RandomDiscrete")

    val model = search.fit(dataset)
    model.transform(dataset).collect()
  }

  test("H2O GridSearch throws exception on invalid hyper parameter") {
    val grid = new H2OGridSearch()
      .setAlgo(
        new H2OGBM()
          .setFeaturesCols(Array("AGE", "RACE", "DPROS", "DCAPS", "PSA", "VOL", "GLEASON"))
          .setLabelCol("CAPSULE"))
      .setHyperParameters(Map("iDoNotExist" -> Array(10, 50).map(_.asInstanceOf[AnyRef])))
    val thrown = intercept[IllegalArgumentException] {
      grid.fit(dataset)
    }
    assert(thrown.getMessage == "Hyper parameter 'iDoNotExist' is not a valid parameter for algorithm 'H2OGBM'")
  }

  private def testGridSearch(
      algo: H2OAlgorithm[_ <: Model.Parameters],
      hyperParams: mutable.HashMap[String, Array[AnyRef]]): Unit = {
    val stage = new H2OGridSearch()
      .setHyperParameters(hyperParams)
      .setAlgo(algo)

    val pipeline = new Pipeline().setStages(Array(stage))
    val algoName = algo.getClass.getSimpleName
    pipeline.write.overwrite().save(s"ml/build/grid_${algoName}_pipeline")
    val loadedPipeline = Pipeline.load(s"ml/build/grid_${algoName}_pipeline")
    val model = loadedPipeline.fit(dataset)

    model.write.overwrite().save(s"ml/build/grid_${algoName}_pipeline_model")
    val loadedModel = PipelineModel.load(s"ml/build/grid_${algoName}_pipeline_model")

    loadedModel.transform(dataset).count()
  }

}
