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

import scala.collection.JavaConverters._

/**
  * This trait contains parameters that are shared across all algorithms.
  */
trait H2OAlgorithmCommonParams extends H2OCommonParams with H2OAlgorithmMOJOParams with H2OBaseMOJOParams {

  //
  // Getters
  //
  override def getFeaturesCols(): Array[String] = {
    val excludedCols = getExcludedCols()
    $(featuresCols).filter(c => excludedCols.forall(e => c.compareToIgnoreCase(e) != 0))
  }

  //
  // Setters
  //

  // Setters for parameters which are defined on MOJO as well
  def setPredictionCol(columnName: String): this.type = set(predictionCol, columnName)

  def setDetailedPredictionCol(columnName: String): this.type = set(detailedPredictionCol, columnName)

  def setWithContributions(enabled: Boolean): this.type = set(withContributions, enabled)

  def setWithLeafNodeAssignments(enabled: Boolean): this.type = set(withLeafNodeAssignments, enabled)

  def setWithStageResults(enabled: Boolean): this.type = set(withStageResults, enabled)

  def setFeaturesCol(first: String): this.type = setFeaturesCols(first)

  def setFeaturesCols(first: String, others: String*): this.type = set(featuresCols, Array(first) ++ others)

  def setFeaturesCols(columnNames: Array[String]): this.type = {
    require(columnNames.length > 0, "Array with feature columns must contain at least one column.")
    set(featuresCols, columnNames)
  }

  def setFeaturesCols(columnNames: java.util.ArrayList[String]): this.type = {
    setFeaturesCols(columnNames.asScala.toArray)
  }

  def setNamedMojoOutputColumns(value: Boolean): this.type = set(namedMojoOutputColumns, value)

  private[sparkling] def getExcludedCols(): Seq[String]
}
