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

package ai.h2o.sparkling.ml.models

import java.sql.{Date, Timestamp}
import ai.h2o.mojos.runtime.utils.MojoDateTime
import ai.h2o.sparkling.SparkTestContext
import org.apache.spark.ml.{Pipeline, PipelineModel}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.Inspectors._
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class H2OMOJOPipelineModelTestSuite extends FunSuite with SparkTestContext with Matchers {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private val PredictionCol = "prediction"
  private lazy val prostateTestData: DataFrame =
    spark.read.option("header", "true").csv("examples/smalldata/prostate/prostate.csv").cache()
  private lazy val prostateMojoPipeline: H2OMOJOPipelineModel = H2OMOJOPipelineModel.createFromMojo(
    this.getClass.getClassLoader.getResourceAsStream("mojo2data/pipeline.mojo"),
    "prostate_pipeline.mojo")

  test("Mojo pipeline can be saved and loaded") {
    val pipeline = new Pipeline().setStages(Array(prostateMojoPipeline))
    pipeline.write.overwrite().save("ml/build/pipeline")
    val loadedPipeline = Pipeline.load("ml/build/pipeline")

    val model = loadedPipeline.fit(prostateTestData)

    model.write.overwrite().save("ml/build/pipeline_model")
    val loadedModel = PipelineModel.load("ml/build/pipeline_model")

    loadedModel.transform(prostateTestData).take(1)
  }

  test("Verify that output columns are correct when using the named columns") {
    val transDf = prostateMojoPipeline.transform(prostateTestData)
    val udfSelection = transDf.select(prostateMojoPipeline.selectPredictionUDF("AGE"))
    val normalSelection = transDf.select("prediction.AGE")

    // Check that frames returned using udf and normal selection are the same
    udfSelection.schema.head.name shouldEqual normalSelection.schema.head.name
    udfSelection.schema.head.dataType shouldEqual normalSelection.schema.head.dataType
    udfSelection.first() shouldEqual normalSelection.first()

    val rows = udfSelection.take(5)
    rows(0).getDouble(0) shouldEqual 65.36339431549945
    rows(1).getDouble(0) shouldEqual 64.98931238070139
    rows(2).getDouble(0) shouldEqual 64.95047899851251
    rows(3).getDouble(0) shouldEqual 65.78738866816514
    rows(4).getDouble(0) shouldEqual 66.11292243968764
  }

  test("Test MOJO with different outputTypes") {
    val testDataDf = spark.read.option("header", "true").csv("examples/smalldata/airlines/airlines_big_data_100.csv")
    val testMojo = H2OMOJOPipelineModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("daiMojos/stringFeaturesOutputPipeline.mojo"),
      "stringFeaturesOutputPipeline.mojo")

    val resultDF = testMojo.transform(testDataDf).select("prediction.*")
    resultDF.collect()

    val columns = resultDF.schema.fields
    val stringColumn = columns.find(_.name.contains("StringConcat")).get
    stringColumn.dataType shouldEqual StringType
    val floatColumn = columns.find(_.name.contains("CatTE")).get
    floatColumn.dataType shouldEqual FloatType
  }

  test("Named columns with multiple output columns") {
    val filePath = getClass.getResource("/mojo2_multiple_outputs/example.csv").getFile
    val df = spark.read.option("header", "true").csv(filePath)
    val mojo = H2OMOJOPipelineModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("mojo2_multiple_outputs/pipeline.mojo"),
      "iris_pipeline.mojo")

    val transDf = mojo.transform(df)
    transDf.select("prediction.*").columns.length shouldEqual 3
    val udfSelection = transDf.select(mojo.selectPredictionUDF("class.Iris-setosa"))
    val normalSelection = transDf.select("prediction.`class.Iris-setosa`") // we need to use ` as the dot is
    // part of the column name, it does not represent the nested column

    udfSelection.schema.head.name shouldEqual normalSelection.schema.head.name
    udfSelection.schema.head.dataType shouldEqual normalSelection.schema.head.dataType
    udfSelection.first() shouldEqual normalSelection.first()
  }

  test("Selection using udf on non-existent column") {
    val transDf = prostateMojoPipeline.transform(prostateTestData)
    intercept[org.apache.spark.sql.AnalysisException] {
      transDf.select(prostateMojoPipeline.selectPredictionUDF("I_DO_NOT_EXIST")).first()
    }
  }

  test("Testing dataset is missing one of the feature columns") {
    val schema = prostateTestData.drop("AGE").schema
    val rdd = sc.parallelize(Seq(Row("1", "0", "1", "2", "1", "1.4", "0", "6")))
    val testingDF = spark.createDataFrame(rdd, schema)

    val predictionsDF = prostateMojoPipeline.transform(testingDF)

    noException should be thrownBy predictionsDF.collect()
  }

  test("Testing dataset having an extra column should give the same prediction") {
    val rdd = sc.parallelize(Seq(Row("1", "0", "65", "1", "2", "1", "1.4", "0", "6")))
    val recordWithoutAdditionalColumn = spark.createDataFrame(rdd, prostateTestData.schema)
    val recordWithAdditionalColumn = recordWithoutAdditionalColumn.withColumn("extraCol", lit("8"))

    val prediction = prostateMojoPipeline.transform(recordWithoutAdditionalColumn).select(PredictionCol).collect().head
    val predictionWithAdditionalColumn =
      prostateMojoPipeline.transform(recordWithAdditionalColumn).select(PredictionCol).collect().head

    prediction shouldEqual predictionWithAdditionalColumn
  }

  test("Testing dataset with column order mixed should give the same prediction") {
    val rdd = sc.parallelize(Seq(Row("1", "0", "65", "1", "2", "1", "1.4", "0", "6")))
    val record = spark.createDataFrame(rdd, prostateTestData.schema)
    val recordWithDifferentColumnOrder = record.selectExpr(record.columns.reverse: _*).toDF()
    record.columns shouldNot equal(recordWithDifferentColumnOrder.columns)
    record.columns.sorted shouldEqual recordWithDifferentColumnOrder.columns.sorted

    val prediction = prostateMojoPipeline.transform(record).select(PredictionCol).collect().head
    val predictionWithDifferentColumnOrder =
      prostateMojoPipeline.transform(recordWithDifferentColumnOrder).select(PredictionCol).collect().head

    prediction shouldEqual predictionWithDifferentColumnOrder
  }

  test("Testing dataset having an extra column and column order mixed should give the same prediction") {
    val rdd = sc.parallelize(Seq(Row("1", "0", "65", "1", "2", "1", "1.4", "0", "6")))
    val record = spark.createDataFrame(rdd, prostateTestData.schema)
    val recordWithDifferentColumnOrderAndAdditionalColumn =
      record.withColumn("extraCol", lit("8")).selectExpr(record.columns.reverse: _*).toDF()

    val prediction = prostateMojoPipeline.transform(record).select(PredictionCol).collect().head
    val predictionWithAdditionalColumn =
      prostateMojoPipeline
        .transform(recordWithDifferentColumnOrderAndAdditionalColumn)
        .select(PredictionCol)
        .collect()
        .head

    prediction shouldEqual predictionWithAdditionalColumn
  }

  test("Prediction with null as row element") {
    val rdd = sc.parallelize(Seq(Row("1", "0", "65", "1", "2", "1", "1.4", "0", null)))
    val df2 = spark.createDataFrame(rdd, prostateTestData.first().schema)
    val preds = prostateMojoPipeline.transform(df2)
    noException should be thrownBy preds.collect()
  }

  test("Date column conversion from Spark to Mojo") {
    val data = List(Date.valueOf("2016-09-30"))

    val sparkDf =
      spark.createDataFrame(sc.parallelize(data).map(Row(_)), StructType(Seq(StructField("date", DataTypes.DateType))))

    val dt = MojoDateTime.parse(sparkDf.first().getDate(0).toString)

    dt.getYear shouldEqual 2016
    dt.getMonth shouldEqual 9
    dt.getDay shouldEqual 30
    dt.getHour shouldEqual 0
    dt.getMinute shouldEqual 0
    dt.getSecond shouldEqual 0
  }

  test("Timestamp column conversion from Spark to Mojo") {
    val ts = new Timestamp(1526891676)

    val sparkDf = spark.createDataFrame(
      sc.parallelize(List(ts)).map(Row(_)),
      StructType(Seq(StructField("timestamp", DataTypes.TimestampType))))

    val dt = MojoDateTime.parse(sparkDf.first().getTimestamp(0).toString)

    dt.getYear shouldEqual ts.toLocalDateTime.getYear
    dt.getMonth shouldEqual ts.toLocalDateTime.getMonth.getValue
    dt.getDay shouldEqual ts.toLocalDateTime.getDayOfMonth
    dt.getHour shouldEqual ts.toLocalDateTime.getHour
    dt.getMinute shouldEqual ts.toLocalDateTime.getMinute
    dt.getSecond shouldEqual ts.toLocalDateTime.getSecond
  }

  test("Transform and transformSchema methods are aligned") {
    val outputSchema = prostateMojoPipeline.transform(prostateTestData).schema
    val transformedSchema = prostateMojoPipeline.transformSchema(prostateTestData.schema)
    transformedSchema shouldEqual outputSchema
  }

  test("Mojo pipeline can expose contribution (SHAP) values") {
    val mojoSettings = H2OMOJOSettings(withContributions = true)
    val pipeline = H2OMOJOPipelineModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("daiMojoShapley/pipeline.mojo"),
      "pipeline.mojo",
      mojoSettings)

    val df = spark.read.option("header", "true").csv("ml/src/test/resources/daiMojoShapley/example.csv")
    val predictionsAndContributions = pipeline.transform(df)
    val onlyContributions = predictionsAndContributions.select(s"${pipeline.getContributionsCol()}.*").cache()
    val featureColumns = 4
    val classes = 3
    val bias = 1
    val contributionColumnsNo = classes * (featureColumns + bias)

    onlyContributions.columns should have length contributionColumnsNo
    forAll(onlyContributions.columns) { _ should startWith("contrib_") }

    val rows = onlyContributions.take(2)
    val firstRow = rows(0)
    firstRow.getDouble(0) shouldBe 2.556562795555615
    firstRow.getDouble(1) shouldBe -1.8716305396517994
    firstRow.getDouble(2) shouldBe 5.894809161198736
    firstRow.getDouble(3) shouldBe -0.5265544725232273
    firstRow.getDouble(4) shouldBe 0.482689546
    val secondRow = rows(1)
    secondRow.getDouble(0) shouldBe 1.9396974779396057
    secondRow.getDouble(1) shouldBe -3.986468029321516
    secondRow.getDouble(2) shouldBe 6.374321717618108
    secondRow.getDouble(3) shouldBe -0.006928643424840761
    secondRow.getDouble(4) shouldBe 0.482689546
  }

  test("Mojo pipeline can expose internal contribution (SHAP) values") {
    val mojoSettings = H2OMOJOSettings(withInternalContributions = true)
    val pipeline = H2OMOJOPipelineModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("daiMojoShapleyInternal/pipeline.mojo"),
      "pipeline.internal.mojo",
      mojoSettings)

    val df = spark.read.option("header", "true").csv("ml/src/test/resources/daiMojoShapleyInternal/example.csv")
    val predictionsAndContributions = pipeline.transform(df)
    val onlyContributions = predictionsAndContributions.select(s"${pipeline.getInternalContributionsCol()}.*")
    val contributionColumnsNo = 115

    onlyContributions.columns should have length contributionColumnsNo
    forAll(onlyContributions.columns) { _ should startWith("contrib_") }

    val contributions = onlyContributions.take(2)
    contributions(0).toSeq should not equal contributions(1).toSeq
  }

  test("Transform and transformSchema methods are aligned when (SHAP) contributions are enabled") {
    val mojoSettings = H2OMOJOSettings(withContributions = true)
    val pipeline = H2OMOJOPipelineModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("daiMojoShapley/pipeline.mojo"),
      "pipeline.mojo",
      mojoSettings)

    val df = spark.read.option("header", "true").csv("ml/src/test/resources/daiMojoShapley/example.csv")
    val outputSchema = pipeline.transform(df).schema
    val transformedSchema = pipeline.transformSchema(df.schema)

    transformedSchema should equal(outputSchema)
  }

  test("Transform and transformSchema methods are aligned when (SHAP) internal contributions are enabled") {
    val mojoSettings = H2OMOJOSettings(withInternalContributions = true)
    val pipeline = H2OMOJOPipelineModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("daiMojoShapleyInternal/pipeline.mojo"),
      "pipeline.internal.mojo",
      mojoSettings)

    val df = spark.read.option("header", "true").csv("ml/src/test/resources/daiMojoShapleyInternal/example.csv")
    val outputSchema = pipeline.transform(df).schema
    val transformedSchema = pipeline.transformSchema(df.schema)

    transformedSchema should equal(outputSchema)
  }

  test(
    "Transform and transformSchema methods generates extra sub columns to prediction column if prediction intervals enabled") {
    val mojoSettings = H2OMOJOSettings(withPredictionInterval = true)
    val pipeline = H2OMOJOPipelineModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("daiPredictionInterval/pipeline.mojo"),
      "prediction.interval.mojo",
      mojoSettings)

    val df = spark.read.option("header", "true").csv("ml/src/test/resources/daiPredictionInterval/example.csv")
    val outputSchema = pipeline.transform(df).schema
    val transformedSchema = pipeline.transformSchema(df.schema)

    val prediction = pipeline.transform(df).select("prediction.*")

    transformedSchema should equal(outputSchema)
    prediction.columns.length shouldBe 3
    prediction.columns.contains("secret_Pressure3pm") shouldBe true
    prediction.columns.contains("secret_Pressure3pm.lower") shouldBe true
    prediction.columns.contains("secret_Pressure3pm.upper") shouldBe true

    val expectedCount = prediction.count()
    prediction.select("secret_Pressure3pm").distinct().count() shouldBe expectedCount
    prediction.select("`secret_Pressure3pm.lower`").distinct().count() shouldBe expectedCount
    prediction.select("`secret_Pressure3pm.upper`").distinct().count() shouldBe expectedCount
  }
}
