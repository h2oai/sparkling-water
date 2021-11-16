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
import hex.{Distribution, DistributionFactory, ModelCategory, MultinomialAucType}
import hex.ModelMetrics.IndependentMetricBuilder
import hex.ModelMetricsBinomial.GenericIndependentMetricBuilderBinomial
import hex.ModelMetricsMultinomial.GenericIndependentMetricBuilderMultinomial
import hex.ModelMetricsOrdinal.GenericIndependentMetricBuilderOrdinal
import hex.ModelMetricsRegression.GenericIndependentMetricBuilderRegression
import hex.generic.GenericModelParameters
import hex.genmodel.MojoModel
import hex.genmodel.easy.{EasyPredictModelWrapper, RowData}
import hex.genmodel.utils.DistributionFamily
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, lit, struct}
import org.apache.spark.sql.types.{DoubleType, StructType}

class H2OSupervisedMOJOModel(override val uid: String) extends H2OAlgorithmMOJOModel(uid) with H2OSupervisedMOJOParams {

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

    unwrapMojoModel().getModelCategory match {
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

  override private[sparkling] def makeMetricBuilder(wrapper: EasyPredictModelWrapper): IndependentMetricBuilder[_] = {
    val distributionParam = getParam("distribution")
    val distributionString = getOrDefault(distributionParam).toString
    val distributionFamily = DistributionFamily.valueOf(distributionString)

    val genericParameters = new GenericModelParameters()
    genericParameters._distribution = distributionFamily

    if (hasParam("huberAlpha")) {
      val huberAlphaParam = getParam("huberAlpha")
      genericParameters._huber_alpha = getOrDefault(huberAlphaParam).asInstanceOf[Double]
    }
    if (hasParam("quantileAlpha")) {
      val quantileAlphaParam = getParam("quantileAlpha")
      genericParameters._quantile_alpha = getOrDefault(quantileAlphaParam).asInstanceOf[Double]
    }
    if (hasParam("tweediePower")) {
      val tweediePowerParam = getParam("tweediePower")
      genericParameters._tweedie_power = getOrDefault(tweediePowerParam).asInstanceOf[Double]
    }
    if (hasParam("customDistributionFunc")) {
      val customDistributionFuncParam = getParam("customDistributionFunc")
      genericParameters._custom_distribution_func = getOrDefault(customDistributionFuncParam).asInstanceOf[String]
    }
    val distribution = DistributionFactory.getDistribution(genericParameters)

    val aucType = if (hasParam("aucType")) {
      val aucTypeParam = getParam("aucType")
      MultinomialAucType.valueOf(getOrDefault(aucTypeParam).asInstanceOf[String])
    } else {
      MultinomialAucType.NONE
    }


    val responseColumn = wrapper.m._responseColumn
    val numberOfClasses = wrapper.m.nclasses()
    val responseDomain = wrapper.m.getDomainValues(responseColumn)
    ModelCategory.valueOf(getModelCategory()) match {
      case ModelCategory.Binomial => new GenericIndependentMetricBuilderBinomial(responseDomain, distributionFamily)
      case ModelCategory.Multinomial =>
        new GenericIndependentMetricBuilderMultinomial(numberOfClasses, responseDomain, aucType)
      case ModelCategory.Regression => new GenericIndependentMetricBuilderRegression(distribution)
      case ModelCategory.Ordinal => new GenericIndependentMetricBuilderOrdinal(numberOfClasses, responseDomain)
    }
  }

  override private[sparkling] def extractActualValues(row: RowData, wrapper: EasyPredictModelWrapper): Array[Float] = {
    val responseColumn = wrapper.m._responseColumn
    val encodedActualValue = wrapper.extractRawDataValue(row, responseColumn)
    val result = new Array[Float](1)
    result(0) = encodedActualValue.toFloat
    result
  }
}

object H2OSupervisedMOJOModel extends H2OSpecificMOJOLoader[H2OSupervisedMOJOModel]
