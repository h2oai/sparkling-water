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

import ai.h2o.sparkling.H2OContext
import ai.h2o.sparkling.ml.internals.H2OModel
import ai.h2o.sparkling.ml.models.{H2OMOJOModel, H2OMOJOSettings}
import ai.h2o.sparkling.ml.params.H2OWord2VecParams
import ai.h2o.sparkling.ml.utils.EstimatorCommonUtils
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.ml.Estimator
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util.{DefaultParamsReadable, DefaultParamsWritable, Identifiable}
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, explode, udf}
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

class H2OWord2Vec(override val uid: String)
  extends Estimator[H2OMOJOModel]
  with H2OWord2VecParams
  with EstimatorCommonUtils
  with DefaultParamsWritable {

  protected final val inputCol = new Param[String](this, "inputCol", "input column name")
  protected final val outputCol = new Param[String](this, "outputCol", "output column name")
  setDefault(outputCol, "H2OWord2Vec_output")

  def this() = this(Identifiable.randomUID(classOf[H2OWord2Vec].getSimpleName))

  private def addValue: UserDefinedFunction =
    udf((array: mutable.WrappedArray[String]) => {
      array.toArray ++ Array("")
    })

  override def fit(dataset: Dataset[_]): H2OMOJOModel = {
    val inputCol = getInputCol
    val ds = dataset
      .filter(col(inputCol).isNotNull)
      .filter(s"size($inputCol) != 0")
      .withColumn(inputCol, addValue(col(inputCol)))
      .withColumn(inputCol, explode(col(inputCol)))
      .select(inputCol)

    if (ds.count() == 0) {
      throw new IllegalArgumentException("Empty DataFrame as an input for the H2OWord2Vec is not supported.")
    }

    val h2oContext = H2OContext.ensure(
      "H2OContext needs to be created in order to train the model. Please create one as H2OContext.getOrCreate().")
    val train = h2oContext.asH2OFrame(ds.toDF())
    train.convertColumnsToStrings(Array(0))
    val params = getH2OAlgorithmParams(train) ++
      Map("training_frame" -> train.frameId, "model_id" -> convertModelIdToKey(getModelId()))
    val modelId = trainAndGetDestinationKey(s"/3/ModelBuilders/word2vec", params)
    deleteRegisteredH2OFrames()
    val settings = H2OMOJOSettings(predictionCol = getOutputCol)
    H2OModel(modelId)
      .toMOJOModel(Identifiable.randomUID(classOf[H2OWord2Vec].getSimpleName), settings)
  }

  @DeveloperApi
  override def transformSchema(schema: StructType): StructType = {
    require(getInputCol != null, "Input column can't be null!")
    require(getOutputCol != null, "Output column can't be null!")
    val fields = schema.fields
    val fieldNames = fields.map(_.name)
    require(
      fieldNames.contains(getInputCol),
      s"The specified input column '$inputCol' was not found in the input dataset!")
    require(
      getInputCol != getOutputCol,
      s"""Input column is same as the output column. There can't be an overlap.""".stripMargin)
    require(
      !fieldNames.contains(getOutputCol),
      s"The output column $getOutputCol is present already in the input dataset.")
    schema
  }

  override def copy(extra: ParamMap): this.type = defaultCopy(extra)

  // Getters
  final def getInputCol: String = $(inputCol)

  final def getOutputCol: String = $(outputCol)

  // Setters
  def setInputCol(value: String): this.type = set(inputCol, value)

  def setOutputCol(value: String): this.type = set(outputCol, value)

}

object H2OWord2Vec extends DefaultParamsReadable[H2OWord2Vec]
