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

import java.io.ObjectInputStream

import ai.h2o.sparkling.utils.ScalaUtils.withResource
import hex.genmodel.easy.{EasyPredictModelWrapper, RowData}
import org.scalatest.{FunSuite, Matchers}

class H2OMOJOModelSparkRuntimeIndependencyTestSuite extends FunSuite with Matchers {
  test("Score with internal MOJO without Spark runtime") {
    val swMojoModel =
      withResource(this.getClass.getClassLoader.getResourceAsStream("SerializedH2OMOJOModel")) { inputStream =>
        withResource(new ObjectInputStream(inputStream)) { objectInputStream =>
          objectInputStream.readObject().asInstanceOf[H2OMOJOModel]
        }
      }

    val h2oMojoModel = swMojoModel.unwrapMojoModel()

    val config = new EasyPredictModelWrapper.Config()
    config.setModel(h2oMojoModel)
    val wrapper = new EasyPredictModelWrapper(config)

    val rowData = new RowData()
    rowData.put("ID", "1")
    rowData.put("AGE", "65")
    rowData.put("RACE", "1")
    rowData.put("DPROS", "2")
    rowData.put("DCAPS", "1")
    rowData.put("PSA", "1.4")
    rowData.put("VOL", "0")
    rowData.put("GLEASON", "6")

    val prediction = wrapper.predictBinomial(rowData)
    prediction.classProbabilities(0) shouldBe >(0.0)
    prediction.classProbabilities(1) shouldBe >(0.0)

    val domainValues = swMojoModel.getDomainValues()
    domainValues.size shouldBe >(0)
    domainValues.get("CAPSULE").get shouldEqual Array("0", "1")
  }
}
