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
import hex.genmodel.attributes.parameters.KeyValue
import org.apache.spark.expose.Logging

import scala.collection.JavaConverters._

trait HasMonotoneConstraintsOnMOJO extends ParameterConstructorMethods with SpecificMOJOParameters with Logging {
  private val monotoneConstraints = new NullableMapStringDoubleParam(
    this,
    "monotoneConstraints",
    "A key must correspond to a feature name and value could be 1 or -1")

  def getMonotoneConstraints(): Map[String, Double] = $(monotoneConstraints)

  override private[sparkling] def setSpecificParams(h2oMojo: MojoModel): Unit = {
    super.setSpecificParams(h2oMojo)
    try {
      val h2oParameters = h2oMojo._modelAttributes.getModelParameters()
      val h2oParametersMap = h2oParameters.map(i => i.name -> i.actual_value).toMap
      h2oParametersMap.get("monotone_constraints").foreach { value =>
        val objectArray = value.asInstanceOf[Array[AnyRef]]
        val scalaMap = if (objectArray != null) {
          val keyValues = objectArray.map(_.asInstanceOf[KeyValue])
          keyValues.map(kv => kv.getKey -> kv.getValue).toMap[String, Double]
        } else {
          null
        }
        set(monotoneConstraints, scalaMap)
      }
    } catch {
      case e: Throwable => logError("An error occurred during a try to access H2O MOJO parameters.", e)
    }
  }
}
