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

import ai.h2o.sparkling.ml.models.{H2OMOJOModel, H2OMOJOSettings}
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types._
import org.scalatest.{FunSuite, Matchers}

trait TransformSchemaTestSuite extends FunSuite with Matchers {

  protected def dataset: DataFrame

  protected def mojoName: String

  protected def expectedDetailedPredictionCol: StructField

  protected def expectedPredictionCol: StructField

  protected def getWithLeafNodeAssignments: Boolean = false

  protected def getWithStageResults: Boolean = false

  private def loadMojo(settings: H2OMOJOSettings): H2OMOJOModel = {
    val mojo =
      H2OMOJOModel.createFromMojo(this.getClass.getClassLoader.getResourceAsStream(mojoName), mojoName, settings)
    mojo
  }

  test("transformSchema with detailed prediction col") {
    val model = loadMojo(
      H2OMOJOSettings(withLeafNodeAssignments = getWithLeafNodeAssignments, withStageResults = getWithStageResults))

    val datasetFields = dataset.schema.fields
    val expectedSchema = StructType(datasetFields ++ (expectedDetailedPredictionCol :: expectedPredictionCol :: Nil))
    val expectedSchemaByTransform = model.transform(dataset).schema
    val schema = model.transformSchema(dataset.schema)
    schema shouldEqual expectedSchema
    schema shouldEqual expectedSchemaByTransform
  }
}
