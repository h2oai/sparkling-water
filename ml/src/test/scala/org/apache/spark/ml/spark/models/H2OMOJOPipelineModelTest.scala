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

package org.apache.spark.ml.spark.models

import java.sql.{Date, Timestamp}

import ai.h2o.mojos.runtime.utils.MojoDateTime
import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SparkTestContext
import org.apache.spark.ml.h2o.models.H2OMOJOPipelineModel
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import org.junit.runner.RunWith
import org.scalatest.FunSuite
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class H2OMOJOPipelineModelTest extends FunSuite with SparkTestContext {

  override def beforeAll(): Unit = {
    sc = new SparkContext("local[*]", "test-local", conf = defaultSparkConf)
    super.beforeAll()
  }

  test("Test columns names and numbers") {
    val df = spark.read.option("header", "true").option("inferSchema", true).csv("examples/smalldata/prostate/prostate.csv")

    val mojo = H2OMOJOPipelineModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("mojo2data/pipeline.mojo"),
      "prostate_pipeline.mojo")
    val rawMojo = mojo.getOrCreateModel()

    val mojoInputCols = (0 until rawMojo.getInputMeta.size()).map(rawMojo.getInputMeta.getColumnName(_))
    val mojoInputTypes = (0 until rawMojo.getInputMeta.size()).map(rawMojo.getInputMeta.getColumnType(_).javatype)
    val dfTypes = df.dtypes.filter(_._1 != "AGE").map{case (_, typ) => sparkTypeToMojoType(typ)}.toSeq

    assert(rawMojo.getInputMeta.size() == df.columns.length -1) // response column is not on the input
    assert(mojoInputCols == df.columns.filter(_ != "AGE").toSeq)
    assert(mojoInputTypes == dfTypes)

    assert(rawMojo.getOutputMeta.size() == 1)
    assert(rawMojo.getOutputMeta.getColumnName(0) == "AGE")
    assert(rawMojo.getOutputMeta.getColumnType(0).javatype == "double") // Spark type is int, byt the prediction can be decimal
  }


  private def sparkTypeToMojoType(sparkType: String): String = {
    sparkType match {
      case "IntegerType" => "int"
      case "DoubleType" => "double"
    }
  }

  test("Prediction on Mojo Pipeline using internal API") {
    // Test data
    val df = spark.read.option("header", "true").csv("examples/smalldata/prostate/prostate.csv")
    // Test mojo
    val mojo = H2OMOJOPipelineModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("mojo2data/pipeline.mojo"),
      "prostate_pipeline.mojo")
    val rawMojo = mojo.getOrCreateModel()

    val icolNames = (0 until rawMojo.getInputMeta.size()).map(i => rawMojo.getInputMeta.getColumnName(i))
    val icolTypes = (0 until rawMojo.getInputMeta.size()).map(i => rawMojo.getInputMeta.getColumnType(i))
    val ocolNames = (0 until rawMojo.getOutputMeta.size()).map(i => rawMojo.getOutputMeta.getColumnName(i))
    val ocolTypes = (0 until rawMojo.getOutputMeta.size()).map(i => rawMojo.getOutputMeta.getColumnType(i))
    println("\nMOJO Inputs:")
    println(icolNames.zip(icolTypes).map { case (n, t) => s"${n}[${t}]" }.mkString(", "))
    println("\nMOJO Outputs:")
    println(ocolNames.zip(ocolTypes).map { case (n, t) => s"${n}[${t}]" }.mkString(", "))


    val transDf = mojo.transform(df)
    val udfSelection = transDf.select(mojo.selectPredictionUDF("AGE"))
    val normalSelection = transDf.select("prediction.preds")

    println(s"\n\nSpark Transformer Output:\n${transDf.dtypes.map { case (n, t) => s"${n}[${t}]" }.mkString(" ")}")

    println("Predictions from normal selection:")
    val valuesNormalSelection = normalSelection.take(5)
    assertPredictedValues(valuesNormalSelection)
    println(valuesNormalSelection.mkString("\n"))

    // Verify also output of the udf prediction method. The UDF method always returns one column with correct name

    println("Predictions from udf selection:")
    val valuesUdfSelection = udfSelection.take(5)
    assertPredictedValuesForNamedCols(valuesUdfSelection)
    println(valuesUdfSelection.mkString("\n"))
  }

  test("Verify that output columns are correct when using the named columns") {
    // Test data
    val df = spark.read.option("header", "true").csv("examples/smalldata/prostate/prostate.csv")
    // Test mojo
    val mojo = H2OMOJOPipelineModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("mojo2data/pipeline.mojo"),
      "prostate_pipeline.mojo")
    mojo.setNamedMojoOutputColumns(true)

    val transDf = mojo.transform(df)
    val udfSelection = transDf.select(mojo.selectPredictionUDF("AGE"))
    val normalSelection = transDf.select("prediction.AGE")

    // Check that frames returned using udf and normal selection are the same
    assert(udfSelection.schema.head.name==normalSelection.schema.head.name)
    assert(udfSelection.schema.head.dataType==normalSelection.schema.head.dataType)
    assert(udfSelection.first()==normalSelection.first())

    println("Predictions:")
    assertPredictedValuesForNamedCols(udfSelection.take(5))
  }

  test("Named columns with multiple output columns") {

    val filePath = getClass.getResource("/mojo2_multiple_outputs/example.csv").getFile
    val df = spark.read.option("header", "true").csv(filePath)
    import org.apache.spark.ml.h2o.models._
    val mojo = H2OMOJOPipelineModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("mojo2_multiple_outputs/pipeline.mojo"),
      "iris_pipeline.mojo")
    mojo.setNamedMojoOutputColumns(true)
    assert(mojo.getOutputNames().length == 3)

    val transDf = mojo.transform(df)
    val udfSelection = transDf.select(mojo.selectPredictionUDF("class.Iris-setosa"))
    val normalSelection = transDf.select("prediction.`class.Iris-setosa`") // we need to use ` as the dot is
    // part of the column name, it does not represent the nested column

    // Check that frames returned using udf and normal selection are the same
    assert(udfSelection.schema.head.name==normalSelection.schema.head.name)
    assert(udfSelection.schema.head.dataType==normalSelection.schema.head.dataType)
    assert(udfSelection.first()==normalSelection.first())

  }
  test("Selection using udf on non-existent column"){
    val df = spark.read.option("header", "true").csv("examples/smalldata/prostate/prostate.csv")
    // Test mojo
    val mojo = H2OMOJOPipelineModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("mojo2data/pipeline.mojo"),
      "prostate_pipeline.mojo")
    mojo.setNamedMojoOutputColumns(true)

    val transDf = mojo.transform(df)
    intercept[IllegalArgumentException] {
      transDf.select(mojo.selectPredictionUDF("I_DO_NOT_EXIST")).first()
    }
  }


  /**
    * The purpose of this test is to simply pass and don't throw NullPointerException
    */
  test("Prediction with null as row element") {
    val df = spark.read.option("header", "true").csv("examples/smalldata/prostate/prostate.csv")
    // Test mojo
    val mojo = H2OMOJOPipelineModel.createFromMojo(
      this.getClass.getClassLoader.getResourceAsStream("mojo2data/pipeline.mojo"),
      "prostate_pipeline.mojo")
    val rdd = sc.parallelize(Seq(Row("1", "0", "65", "1", "2", "1", "1.4", "0", null)))
    val df2 = spark.createDataFrame(rdd, df.first().schema)
    val preds = mojo.transform(df2)
    // materialize the frame to see that it is passing
    preds.collect()
  }


  test("Date column conversion from Spark to Mojo"){
    val data = List(Date.valueOf("2016-09-30"))

    val sparkDf = spark.createDataFrame(
      sc.parallelize(data).map(Row(_)),
      StructType(Seq(StructField("date", DataTypes.DateType)))
    )


    val r = sparkDf.first()
    val dt = MojoDateTime.parse(r.getDate(0).toString)

    assert(dt.getYear == 2016)
    assert(dt.getMonth == 9)
    assert(dt.getDay == 30)
    assert(dt.getHour == 0)
    assert(dt.getMinute == 0)
    assert(dt.getSecond == 0)
  }

  test("Timestamp column conversion from Spark to Mojo"){
    System.currentTimeMillis()
    val ts = new Timestamp(System.currentTimeMillis())
    val data = List(ts)

    val sparkDf = spark.createDataFrame(
      sc.parallelize(data).map(Row(_)),
      StructType(Seq(StructField("timestamp", DataTypes.TimestampType)))
    )


    val r = sparkDf.first()
    val dt = MojoDateTime.parse(r.getTimestamp(0).toString)
    import java.util.Calendar
    val cal = Calendar.getInstance
    cal.setTimeInMillis(ts.getTime)
    assert(dt.getYear == cal.get(Calendar.YEAR))
    assert(dt.getMonth == cal.get(Calendar.MONTH) + 1)
    assert(dt.getDay == cal.get(Calendar.DAY_OF_MONTH))
    assert(dt.getHour == cal.get(Calendar.HOUR_OF_DAY))
    assert(dt.getMinute == cal.get(Calendar.MINUTE))
    assert(dt.getSecond == cal.get(Calendar.SECOND))
  }

  private def assertPredictedValues(preds: Array[Row]): Unit = {
    assert(preds(0).getSeq[Double](0).head == 65.36320409515132)
    assert(preds(1).getSeq[Double](0).head == 64.96902128114817)
    assert(preds(2).getSeq[Double](0).head == 64.96721023747583)
    assert(preds(3).getSeq[Double](0).head == 65.78772654671035)
    assert(preds(4).getSeq[Double](0).head == 66.11327967814829)
  }

  private def assertPredictedValuesForNamedCols(preds: Array[Row]): Unit = {
    assert(preds(0).getDouble(0) == 65.36320409515132)
    assert(preds(1).getDouble(0) == 64.96902128114817)
    assert(preds(2).getDouble(0) == 64.96721023747583)
    assert(preds(3).getDouble(0) == 65.78772654671035)
    assert(preds(4).getDouble(0) == 66.11327967814829)
  }
}
