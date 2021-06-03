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

import ai.h2o.sparkling.ml.params.H2OAutoEncoderMOJOParams
import org.apache.spark.ml.PipelineStage
import org.apache.spark.sql.types.{ArrayType, DoubleType, StructField, StructType}

trait H2OAutoEncoderBase extends PipelineStage with H2OAutoEncoderMOJOParams{
  override def transformSchema(schema: StructType): StructType = {
    validateSchema(schema)
    val originalField = StructField("original", ArrayType(DoubleType, containsNull = false), nullable = true)
    val reconstructedField = StructField("reconstructed", ArrayType(DoubleType, containsNull = false), nullable = true)
    val reconstructionErrorField = StructField("mse", DoubleType, nullable = false)
    val fields = originalField :: reconstructedField :: reconstructionErrorField :: Nil


    StructType(schema.fields ++ fields)
  }

  private def validateSchema(flatSchema: StructType): Unit = {
    require(getInputCols() != null && getInputCols().nonEmpty, "The list of input columns can't be null or empty!")

    val fields = flatSchema.fields
    val fieldNames = fields.map(_.name)

    getInputCols().foreach { inputCol =>
      require(
        fieldNames.contains(inputCol),
        s"The specified input column '$inputCol' was not found in the input dataset!")
    }
  }
}
