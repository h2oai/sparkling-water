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

package ai.h2o.sparkling.ml.features

import ai.h2o.sparkling.ml.algos.H2OKMeans
import ai.h2o.sparkling.ml.models.H2OMOJOModel
import ai.h2o.sparkling.{SharedH2OTestContext, TestUtils}
import org.apache.spark.ml.Pipeline
import org.apache.spark.ml.feature.StopWordsRemover.loadDefaultStopWords
import org.apache.spark.ml.feature.{RegexTokenizer, StopWordsRemover}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, collect_set, size => array_size}
import org.apache.spark.sql.types.{ArrayType, FloatType, StructField, StructType}
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{FunSuite, Matchers}

import scala.collection.mutable

@RunWith(classOf[JUnitRunner])
class H2OWord2VecTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")

  private lazy val craigslistJobTitles = spark.read
    .option("header", "true")
    .option("inferSchema", "true")
    .csv(TestUtils.locate("smalldata/craigslistJobTitles.csv"))
    .select("jobtitle")

  def similarJobTitlesPipeline(): Pipeline = {
    val tokenizer = new RegexTokenizer()
      .setInputCol("jobtitle")
      .setGaps(false)
      .setToLowercase(true)
      .setPattern("[a-zA-Z]+")
      .setMinTokenLength(2)

    val jobListingsStopWords =
      Array("hiring", "job", "fulltime", "parttime", "opening", "part", "time", "full", "needed", "seeking", "seek")

    val stopWordsRemover = new StopWordsRemover()
      .setStopWords(loadDefaultStopWords("english") ++ jobListingsStopWords)
      .setInputCol(tokenizer.getOutputCol)

    val w2v = new H2OWord2Vec()
      .setSentSampleRate(0)
      .setEpochs(10)
      .setVecSize(101)
      .setOutputCol("Word2VecOutput")
      .setInputCol(stopWordsRemover.getOutputCol)

    val kMeans = new H2OKMeans()
      .setK(100)
      .setSeed(42)
      .setFeaturesCol(w2v.getOutputCol())
      .setPredictionCol("cluster")

    new Pipeline().setStages(Array(tokenizer, stopWordsRemover, w2v, kMeans))
  }

  test("Word2Vec should put related sentences in close proximity") {
    val model = similarJobTitlesPipeline().fit(craigslistJobTitles)
    val result = model.transform(craigslistJobTitles)

    val similarJobs = result
      .groupBy("cluster")
      .agg(collect_set("jobtitle").as("jobs"))
      .sort(array_size(col("jobs")).desc)
      .select("jobs")
      .collect()
      .map(_.getAs[mutable.WrappedArray[String]](0))

    relatedJobTitleVectorsShouldBeCloseToEachOther("customer", "sales", "service")
    relatedJobTitleVectorsShouldBeCloseToEachOther("teacher", "school", "teaching")
    relatedJobTitleVectorsShouldBeCloseToEachOther("front desk", "receptionist", "office")

    def relatedJobTitleVectorsShouldBeCloseToEachOther(
        jobTitleGroupKeyword: String,
        otherKeywordsExpectedTogether: String*): Unit = {
      val mainKeywordJobTitleGroup =
        similarJobs
          .find(jobList => jobList.count(_.toLowerCase.contains(jobTitleGroupKeyword)) > (jobList.size / 2))
          .get
          .map(_.toLowerCase())
      val jobTitlesWithoutTheMainKeyword = mainKeywordJobTitleGroup.filterNot(_.contains(jobTitleGroupKeyword))
      otherKeywordsExpectedTogether.foreach { expectedKeyword =>
        jobTitlesWithoutTheMainKeyword.exists(_.contains(expectedKeyword)) shouldBe true
      }
    }
  }

  test("should not accept an empty dataset") {
    import spark.implicits._
    val df = Seq.empty[Array[String]].toDF()

    val w2v = new H2OWord2Vec().setInputCol("value")

    val thrown = intercept[IllegalArgumentException] {
      w2v.fit(df)
    }
    assert(thrown.getMessage == "Empty DataFrame as an input for the H2OWord2Vec is not supported.")
  }

  test("should not accept a dataset with a null column") {
    import spark.implicits._
    val df = spark.sparkContext.parallelize(Seq(null.asInstanceOf[Array[String]])).toDF()

    val w2v = new H2OWord2Vec().setInputCol("value")

    val thrown = intercept[IllegalArgumentException] {
      w2v.fit(df)
    }
    assert(thrown.getMessage == "Empty DataFrame as an input for the H2OWord2Vec is not supported.")
  }

  test("should not accept empty dataset for fitting") {
    import spark.implicits._
    val df = spark.sparkContext.parallelize(Seq(Array.empty[String])).toDS()

    val w2v = new H2OWord2Vec().setInputCol("value")

    val thrown = intercept[IllegalArgumentException] {
      w2v.fit(df)
    }
    assert(thrown.getMessage == "Empty DataFrame as an input for the H2OWord2Vec is not supported.")
  }

  private def truncateAt(n: Double, p: Int): Double = { val s = math pow (10, p); (math floor n * s) / s }

  test("should not fail when null value present") {
    import spark.implicits._
    val df = spark.sparkContext.parallelize(Seq(null.asInstanceOf[Array[String]], Array("how", "are", "you"))).toDF()

    val w2v = new H2OWord2Vec().setMinWordFreq(1).setVecSize(3).setInputCol("value")
    val model = w2v.fit(df)
    val result = model.transform(df).select("value", w2v.getOutputCol).collect()
    assert(result(0)(0) == null)
    assert(result(0)(1) == null)
    assert(result(1)(0) == mutable.WrappedArray.make(Array("how", "are", "you")))
    val embeddings = result(1).getAs[mutable.WrappedArray[Float]](1)
    assert(truncateAt(embeddings(0), 3).toString == "-0.062")
    assert(truncateAt(embeddings(1), 3).toString == "0.037")
    assert(truncateAt(embeddings(2), 3).toString == "0.002")
  }

  test("should not fail when empty array present") {
    import spark.implicits._
    val df = spark.sparkContext.parallelize(Seq(Array.empty[String], Array("how", "are", "you"))).toDF()

    val w2v = new H2OWord2Vec().setMinWordFreq(1).setVecSize(3).setInputCol("value")
    w2v.fit(df)
    val model = w2v.fit(df)
    val result = model.transform(df).select("value", w2v.getOutputCol).collect()
    assert(result(0)(0) == mutable.WrappedArray.empty)
    val emptyValues = result(0).getAs[mutable.WrappedArray[Float]](1)
    assert(emptyValues(0).isNaN)
    assert(emptyValues(1).isNaN)
    assert(emptyValues(2).isNaN)
    assert(result(1)(0) == mutable.WrappedArray.make(Array("how", "are", "you")))
    val embeddings = result(1).getAs[mutable.WrappedArray[Float]](1)
    assert(truncateAt(embeddings(0), 3).toString == "-0.062")
    assert(truncateAt(embeddings(1), 3).toString == "0.037")
    assert(truncateAt(embeddings(2), 3).toString == "0.002")
  }

  test("should not accept column which is not string type for fitting") {
    import spark.implicits._
    val input = Seq(1, 2, 3, 4, 5).toDS
    val w2v = new H2OWord2Vec().setMinWordFreq(1).setVecSize(3).setInputCol("value")

    val thrown = intercept[IllegalArgumentException] {
      w2v.fit(input)
    }

    thrown.getMessage shouldBe "The specified input column 'value' type ('IntegerType') is not an array of strings!"
  }

  test("should not accept column which is not string type for scoring") {
    import spark.implicits._
    val input = Seq(Seq("a", "b", "c")).toDS
    val w2v = new H2OWord2Vec().setMinWordFreq(1).setVecSize(3).setInputCol("value")

    val model = w2v.fit(input)

    val thrown = intercept[IllegalArgumentException] {
      model.transform(Seq(1, 2, 3).toDF)
    }

    thrown.getMessage shouldBe "The specified input column 'value' type ('IntegerType') is not an array of strings!"
  }

  test("should load from mojo and return proper schema") {
    import spark.implicits._
    val input = Seq(Seq("a", "b", "c")).toDF("Words")
    val model =
      H2OMOJOModel.createFromMojo(this.getClass.getClassLoader.getResourceAsStream("word2vec.mojo"), "word2vec.mojo")
    val expectedPredictionColType = ArrayType(FloatType, containsNull = false)
    val expectedPredictionCol = StructField("word2vec.mojo__output", expectedPredictionColType, nullable = true)
    val datasetFields = input.schema.fields
    val expectedSchema = StructType(datasetFields :+ expectedPredictionCol)
    val expectedSchemaByTransform = model.transform(input).schema
    val schema = model.transformSchema(input.schema)
    schema shouldEqual expectedSchema
    schema shouldEqual expectedSchemaByTransform
  }
}
