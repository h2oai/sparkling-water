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

import org.apache.spark.ml.param.{BooleanParam, Param}
import org.apache.spark.sql.types.{StructField, StructType}

trait H2OGLRMExtraParams extends H2ODimReductionExtraParams {

  private val reconstructedCol: Param[String] = new Param[String](
    parent = this,
    name = "reconstructedCol",
    doc = "Reconstructed column name. This column contains reconstructed input values (A_hat=X*Y instead of just X).")

  private val withReconstructedCol: Param[Boolean] = new BooleanParam(
    parent = this,
    name = "withReconstructedCol",
    doc = "A flag identifying whether a column with reconstructed input values will be produced or not.")

  setDefault(reconstructedCol -> (uid + "__reconstructed"), withReconstructedCol -> false)

  //
  // Getters
  //
  def getReconstructedCol(): String = $(reconstructedCol)

  def getWithReconstructedCol(): Boolean = $(withReconstructedCol)

  //
  // Setters
  //
  def setReconstructedCol(name: String): this.type = set(reconstructedCol -> name)

  def setWithReconstructedCol(flag: Boolean): this.type = set(withReconstructedCol -> flag)

  protected override def outputSchema: Seq[StructField] = {
    val outputType = org.apache.spark.ml.linalg.SQLDataTypes.VectorType
    val baseSchema = super.outputSchema

    val withReconstructedFieldSchema = if (getWithReconstructedCol()) {
      val reconstructedField = StructField(getReconstructedCol(), outputType, nullable = false)
      baseSchema :+ reconstructedField
    } else {
      baseSchema
    }

    withReconstructedFieldSchema
  }

  protected override def validate(schema: StructType): Unit = {
    super.validate(schema)
    val fieldNames = schema.fieldNames

    require(getReconstructedCol() != null || !getWithReconstructedCol(), "The reconstructed column can't be null!")
    require(
      !fieldNames.contains(getReconstructedCol()) || !getWithReconstructedCol(),
      s"The reconstructed column '${getReconstructedCol()}' is already present in the dataset!")
  }

  protected override def copyExtraParams(to: H2ODimReductionExtraParams): Unit = {
    super.copyExtraParams(to)
    val toGLRM = to.asInstanceOf[H2OGLRMExtraParams]
    toGLRM.setReconstructedCol(getReconstructedCol())
    toGLRM.setWithReconstructedCol(getWithReconstructedCol())
  }
}
