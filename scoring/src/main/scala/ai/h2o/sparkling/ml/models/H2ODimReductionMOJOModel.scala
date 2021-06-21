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

import ai.h2o.sparkling.ml.params.H2ODimReductionExtraParams
import ai.h2o.sparkling.sql.functions.udf
import org.apache.spark.ml.linalg.DenseVector
import org.apache.spark.sql.Row
import org.apache.spark.sql.catalyst.expressions.GenericRowWithSchema
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.types.StructType

trait H2ODimReductionMOJOModel extends H2OFeatureMOJOModel with H2ODimReductionExtraParams {

  override protected def inputColumnNames: Array[String] = getInputCols()

  protected override def mojoUDF: UserDefinedFunction = {
    val schema = StructType(outputSchema)
    val function = (r: Row) => {
      val model = H2OMOJOCache.getMojoBackend(uid, getMojo, this)
      val pred = model.predictDimReduction(RowConverter.toH2ORowData(r))
      val rowData = Array[Any](new DenseVector(pred.dimensions).compressed)
      new GenericRowWithSchema(rowData, schema)
    }
    udf(function, schema)
  }
}
