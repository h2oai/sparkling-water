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

import java.io.File

import ai.h2o.sparkling.SharedH2OTestContext
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class H2OWord2VecTokenizerTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkSession(): SparkSession = sparkSession("local[*]")
  import spark.implicits._

  private def loadTitlesTable(spark: SparkSession): DataFrame = {
    val titlesDataPath = "./examples/smalldata/craigslistJobTitles.csv"
    val titlesDataFile = s"file://${new File(titlesDataPath).getAbsolutePath}"
    spark.read.option("inferSchema", "true").option("header", "true").csv(titlesDataFile)
  }

  private lazy val dataset = loadTitlesTable(spark)

  private def getReferenceTokenizer() = {
    new H2OWord2VecTokenizer()
      .setInputCol("jobtitle")
      .setOutputCol("tokenized")
  }

  test("transform schema") {
    val tokenizer = getReferenceTokenizer()
    val transformedSchema = tokenizer.transformSchema(dataset.schema)
    val schemaOfTransformedDataset = tokenizer.transform(dataset).schema
    assert(transformedSchema.fields.sameElements(schemaOfTransformedDataset.fields))
  }

  test("transform data") {
    val tokenizer = getReferenceTokenizer()
    val data = tokenizer.transform(dataset).select("tokenized").map(row => row.getString(0))
    val actual = data.collect().take(20)
    val expected =
      Array(
        "after",
        "school",
        "supervisor",
        "",
        "*****tutors",
        "needed",
        "-",
        "for",
        "all",
        "subjects,",
        "all",
        "ages*****",
        "",
        "bay",
        "area",
        "family",
        "recruiter",
        "",
        "adult",
        "day")
    assert(actual.sameElements(expected))
  }

  test("Empty input dataset") {
    val tokenizer = getReferenceTokenizer()
    val emptyDataset = dataset.filter(_ => false)
    val result = tokenizer.transform(emptyDataset).map(row => row.getString(0)).collect()
    assert(result.isEmpty)
  }

  test("No input column specified") {
    val thrown = intercept[IllegalArgumentException] {
      new H2OWord2VecTokenizer().setOutputCol("dummy") transform (dataset)
    }
    assert(thrown.getMessage == "requirement failed: Input column has to be specified!")
  }

  test("No output column specified") {
    val thrown = intercept[IllegalArgumentException] {
      new H2OWord2VecTokenizer().setInputCol("dummy") transform (dataset)
    }
    assert(thrown.getMessage == "requirement failed: Output column has to be specified!")
  }
}
