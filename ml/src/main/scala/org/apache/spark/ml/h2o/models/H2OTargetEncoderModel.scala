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
package org.apache.spark.ml.h2o.models

import org.apache.spark.ml.Model
import org.apache.spark.ml.h2o.param.H2OTargetEncoderParams
import org.apache.spark.sql.{DataFrame, Dataset}
import org.apache.spark.sql.functions._

class H2OTargetEncoderModel(
    override val uid: String,
    encodingMap: Map[String, Map[String, Array[Int]]])
  extends Model[H2OTargetEncoderModel] with H2OTargetEncoderParams {

  override def transform(dataset: Dataset[_]): DataFrame = {
    // TODO The way how to apply encoding table for individual records should be defined in H2O-3 and exposed out.
    getInputCols().zip(getOutputCols()).foldLeft(dataset.toDF()){
      case (df, (in, out)) => df.withColumn(out, col(in))
    }
  }
}
