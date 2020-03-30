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

import org.apache.spark.SparkContext
import org.apache.spark.h2o.utils.SharedH2OTestContext
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.ParamMap
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.scalatest.{FunSuite, Matchers}

class H2OAlgoCommonUtilsTestSuite extends FunSuite with Matchers with SharedH2OTestContext {

  override def createSparkContext = new SparkContext("local[*]", this.getClass.getSimpleName, conf = defaultSparkConf)

  val datasetSchema = (new StructType)
    .add("preds.probability", "int", true)
    .add("39_ClusterDist6:PAY_0.9", "double", true)
    .add("35_TruncSVD:AGE:BILL_AMT3:BILL_AMT4:PAY_3:PAY_6:PAY_AMT4.0", "double", false)

  class DummyTestClass(override val uid: String) extends Transformer with H2OAlgoCommonUtils {

    override def transform(dataset: Dataset[_]): DataFrame = ???

    override def copy(extra: ParamMap): Transformer = ???

    override def transformSchema(schema: StructType): StructType = ???

    override protected def getExcludedCols(): Seq[String] = Nil

    def exposedTestMethod = prepareDatasetForFitting _
  }

  test("Columns sanitation: DAI type of columns names") {
    // Given
    val dataset = spark.createDataFrame(sc.parallelize(1 to 5, 5).map(i => Row(i, 2.0 * i, i.toDouble)), datasetSchema)

    val utils = new DummyTestClass("43")

    // When: transform
    val (trainHf, testHf, internalFeatureCols) = utils.exposedTestMethod(dataset)
    testHf shouldBe None
    internalFeatureCols shouldBe datasetSchema.fields.map(_.name)
  }
}
