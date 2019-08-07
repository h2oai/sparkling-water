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

import ai.h2o.sparkling.ml.models.H2OMOJOPredictionClustering.Base
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.{ArrayType, DoubleType, StructField}
import org.apache.spark.sql.{Column, Row}

trait H2OMOJOPredictionClustering {
  self: H2OMOJOModel =>

  def getClusteringPredictionUDF(): UserDefinedFunction = {
    udf[Base, Row] { r: Row =>
      val pred = easyPredictModelWrapper.predictClustering(RowConverter.toH2ORowData(r))
      Base(pred.cluster, pred.distances)
    }
  }

  def getClusteringPredictionColSchema(): Seq[StructField] = StructField("value", DoubleType) :: Nil

  def getClusteringDetailedPredictionColSchema(): Seq[StructField] = {
    StructField("value", DoubleType) :: StructField("distances", ArrayType(DoubleType)) :: Nil
  }


  def extractClusteringPredictionColContent(): Column = {
    col(s"${getDetailedPredictionCol()}.cluster")
  }
}

object H2OMOJOPredictionClustering {

  case class Base(cluster: Integer, distances: Array[Double])

}
