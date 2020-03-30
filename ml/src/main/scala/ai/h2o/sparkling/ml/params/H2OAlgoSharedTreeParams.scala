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

import ai.h2o.sparkling.ml.params.H2OAlgoParamsHelper.getValidatedEnumValue
import hex.tree.SharedTreeModel.SharedTreeParameters
import hex.tree.SharedTreeModel.SharedTreeParameters.HistogramType
import org.apache.spark.expose.Logging

trait H2OAlgoSharedTreeParams[P <: SharedTreeParameters]
  extends H2OAlgoSupervisedParams[P]
  with H2OTreeBasedSupervisedMOJOParams
  with HasStoppingCriteria[P]
  with Logging {

  //
  // Param definitions
  //
  final val maxDepth = intParam("maxDepth")
  final val minRows = doubleParam("minRows")
  final val nbins = intParam("nbins")
  final val nbinsCats = intParam("nbinsCats")
  final val minSplitImprovement = doubleParam("minSplitImprovement")
  final val histogramType = stringParam("histogramType")
  final val nbinsTopLevel = intParam("nbinsTopLevel")
  final val buildTreeOneNode = booleanParam("buildTreeOneNode")
  final val scoreTreeInterval = intParam("scoreTreeInterval")
  final val sampleRate = doubleParam("sampleRate")
  final val sampleRatePerClass = nullableDoubleArrayParam("sampleRatePerClass")
  final val colSampleRateChangePerLevel = doubleParam("colSampleRateChangePerLevel")
  final val colSampleRatePerTree = doubleParam("colSampleRatePerTree")

  //
  // Default values
  //
  setDefault(
    ntrees -> parameters._ntrees,
    maxDepth -> parameters._max_depth,
    minRows -> parameters._min_rows,
    nbins -> parameters._nbins,
    nbinsCats -> parameters._nbins_cats,
    minSplitImprovement -> parameters._min_split_improvement,
    histogramType -> parameters._histogram_type.name(),
    nbinsTopLevel -> parameters._nbins_top_level,
    buildTreeOneNode -> parameters._build_tree_one_node,
    scoreTreeInterval -> parameters._score_tree_interval,
    sampleRate -> parameters._sample_rate,
    sampleRatePerClass -> parameters._sample_rate_per_class,
    colSampleRateChangePerLevel -> parameters._col_sample_rate_change_per_level,
    colSampleRatePerTree -> parameters._col_sample_rate_per_tree)

  //
  // Getters
  //
  def getMaxDepth() = $(maxDepth)

  def getMinRows() = $(minRows)

  def getNbins() = $(nbins)

  def getNbinsCats() = $(nbinsCats)

  def getMinSplitImprovement() = $(minSplitImprovement)

  def getHistogramType() = $(histogramType)

  def getNbinsTopLevel() = $(nbinsTopLevel)

  def getBuildTreeOneNode() = $(buildTreeOneNode)

  def getScoreTreeInterval() = $(scoreTreeInterval)

  def getSampleRate() = $(sampleRate)

  def getSampleRatePerClass() = $(sampleRatePerClass)

  def getColSampleRateChangePerLevel() = $(colSampleRateChangePerLevel)

  def getColSampleRatePerTree() = $(colSampleRatePerTree)

  //
  // Setters
  //
  def setNtrees(value: Int): this.type = set(ntrees, value)

  def setMaxDepth(value: Int): this.type = set(maxDepth, value)

  def setMinRows(value: Double): this.type = set(minRows, value)

  def setNbins(value: Int): this.type = set(nbins, value)

  def setNbinsCats(value: Int): this.type = set(nbinsCats, value)

  def setMinSplitImprovement(value: Double): this.type = set(minSplitImprovement, value)

  def setHistogramType(value: String): this.type = {
    val validated = getValidatedEnumValue[HistogramType](value)
    set(histogramType, validated)
  }

  def setNbinsTopLevel(value: Int): this.type = set(nbinsTopLevel, value)

  def setBuildTreeOneNode(value: Boolean): this.type = set(buildTreeOneNode, value)

  def setScoreTreeInterval(value: Int): this.type = set(scoreTreeInterval, value)

  def setSampleRate(value: Double): this.type = set(sampleRate, value)

  def setSampleRatePerClass(value: Array[Double]): this.type = set(sampleRatePerClass, value)

  def setColSampleRateChangePerLevel(value: Double): this.type = set(colSampleRateChangePerLevel, value)

  def setColSampleRatePerTree(value: Double): this.type = set(colSampleRatePerTree, value)

  override private[sparkling] def getH2OAlgorithmParams(): Map[String, Any] = {
    super.getH2OAlgorithmParams() ++
      Map(
        "ntrees" -> getNtrees(),
        "max_depth" -> getMaxDepth(),
        "min_rows" -> getMinRows(),
        "nbins" -> getNbins(),
        "nbins_cats" -> getNbinsCats(),
        "min_split_improvement" -> getMinSplitImprovement(),
        "histogram_type" -> getHistogramType(),
        "stopping_rounds" -> getStoppingRounds(),
        "stopping_metric" -> getStoppingMetric(),
        "stopping_tolerance" -> getStoppingTolerance(),
        "nbins_top_level" -> getNbinsTopLevel(),
        "build_tree_one_node" -> getBuildTreeOneNode(),
        "score_tree_interval" -> getScoreTreeInterval(),
        "sample_rate" -> getSampleRate(),
        "sample_rate_per_class" -> getSampleRatePerClass(),
        "col_sample_rate_change_per_level" -> getColSampleRateChangePerLevel(),
        "col_sample_rate_per_tree" -> getColSampleRatePerTree())
  }
}
