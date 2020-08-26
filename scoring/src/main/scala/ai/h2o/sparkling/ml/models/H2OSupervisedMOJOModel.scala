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

import ai.h2o.sparkling.ml.params.H2OSupervisedMOJOParams
import hex.ModelCategory
import hex.genmodel.MojoModel
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.types.{DoubleType, StructType}

class H2OSupervisedMOJOModel(override val uid: String) extends H2OMOJOModel(uid) with H2OSupervisedMOJOParams {

  override private[sparkling] def setSpecificParams(mojoModel: MojoModel): Unit = {
    super.setSpecificParams(mojoModel)
    set(offsetCol -> mojoModel._offsetColumn)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    val offsetColumn = getOffsetCol()
    if (offsetColumn != null) {
      require(schema.fieldNames.contains(offsetColumn), "Offset column must be present within the dataset!")
    }
    super.transformSchema(schema)
  }

  protected override def applyPredictionUdfToFlatDataFrame(
      flatDataFrame: DataFrame,
      udfConstructor: Array[String] => UserDefinedFunction,
      inputs: Array[String]): DataFrame = {
    val relevantColumnNames = getRelevantColumnNames(flatDataFrame, inputs)
    val args = relevantColumnNames.map(c => flatDataFrame(s"`$c`"))
    val udf = udfConstructor(relevantColumnNames)
    val predictWrapper = H2OMOJOCache.getMojoBackend(uid, getMojo, this)
    predictWrapper.getModelCategory match {
      case ModelCategory.Binomial | ModelCategory.Regression | ModelCategory.Multinomial | ModelCategory.Ordinal =>
        val offsetColumn = getOffsetCol()
        if (offsetColumn != null) {
          if (!flatDataFrame.columns.contains(offsetColumn)) {
            throw new RuntimeException("Offset column must be present within the dataset!")
          }
          flatDataFrame.withColumn(outputColumnName, udf(struct(args: _*), col(getOffsetCol()).cast(DoubleType)))
        } else {
          // Methods of EasyPredictModelWrapper for given prediction categories take offset as parameter.
          // `lit(0.0)` represents a column with zero values (offset disabled).
          flatDataFrame.withColumn(outputColumnName, udf(struct(args: _*), lit(0.0)))
        }
      case _ =>
        flatDataFrame.withColumn(outputColumnName, udf(struct(args: _*)))
    }
  }
}

object H2OSupervisedMOJOModel extends H2OSpecificMOJOLoader[H2OSupervisedMOJOModel]
