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

import ai.h2o.sparkling.ml.models.H2OWord2VecMOJOModel
import ai.h2o.sparkling.ml.params.H2OWord2VecExtraParams
import hex.Model
import org.apache.spark.sql.Dataset
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, explode, size, udf}

import scala.reflect.ClassTag

abstract class H2OWord2VecBase[P <: Model.Parameters: ClassTag]
  extends H2OFeatureEstimator[P]
  with H2OWord2VecExtraParams {

  override def fit(dataset: Dataset[_]): H2OWord2VecMOJOModel = {
    validate(dataset.schema)
    val appendSentenceDelimiter: UserDefinedFunction = udf[Seq[String], Seq[String]](_ :+ "")
    val inputCol: String = getInputCol()
    val ds = dataset
      .filter(col(inputCol).isNotNull)
      .filter(size(col(inputCol)) =!= 0)
      .withColumn(inputCol, appendSentenceDelimiter(col(inputCol)))
      .withColumn(inputCol, explode(col(inputCol)))
      .select(inputCol)

    if (ds.isEmpty) {
      throw new IllegalArgumentException("Empty DataFrame as an input for the H2OWord2Vec is not supported.")
    }

    val model = super.fit(ds).asInstanceOf[H2OWord2VecMOJOModel]
    copyExtraParams(model)

    model
  }

  override def getColumnsToString(): Array[String] = getInputCols()

  private[sparkling] override def getInputCols(): Array[String] = Array(getInputCol())

  private[sparkling] override def setInputCols(cols: Array[String]) = {
    require(cols.length == 1, "Word2Vec supports only one input column")
    setInputCol(cols.head)
  }

}
