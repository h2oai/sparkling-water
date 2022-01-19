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

import ai.h2o.sparkling.ml.params.H2OWord2VecExtraParams
import ai.h2o.sparkling.sql.functions.udf
import org.apache.spark.sql.{DataFrame, Dataset, Row}
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StructType

import scala.collection.mutable

trait H2OWord2VecMOJOBase extends H2OFeatureMOJOModel with H2OWord2VecExtraParams {

  override protected def inputColumnNames: Array[String] = Array(getInputCol())

  override def transform(dataset: Dataset[_]): DataFrame = {
    validate(dataset.schema)
    super.transform(dataset)
  }

  protected override def mojoUDF: UserDefinedFunction = {
    val schema = StructType(outputSchema)
    val uid = this.uid
    val mojoFileName = this.mojoFileName
    val configInitializers = this.getEasyPredictModelWrapperConfigurationInitializers()
    val inputCol = getInputCol()
    val function = (r: Row) => {
      val model = H2OMOJOModel.loadEasyPredictModelWrapper(uid, mojoFileName, configInitializers)
      val colIdx = model.m.getColIdx(inputCol)
      val pred = if (r.isNullAt(colIdx)) {
        null
      } else {
        model.predictWord2Vec(r.getSeq[String](colIdx).toArray)
      }
      val resultBuilder = mutable.ArrayBuffer[Any]()
      resultBuilder += pred
      new GenericRowWithSchema(resultBuilder.toArray, schema)
    }
    udf(function, schema)
  }

}
