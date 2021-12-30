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
import org.apache.spark.ml.param.{Param, Params}

trait HasInputColOnMOJO extends Params with SpecificMOJOParameters with Logging {
  protected val inputCol: Param[String] = new Param[String](this, "inputCol", "Input column name")

  def getInputCol(): String = $(inputCol)

  def setInputCol(name: String): this.type = set(inputCol -> name)

  override private[sparkling] def setSpecificParams(h2oMojo: MojoModel): Unit = {
    super.setSpecificParams(h2oMojo)
    h2oMojo.features().headOption.foreach { feature =>
      set(inputCol -> feature)
    }
  }

}
