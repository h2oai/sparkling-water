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
package org.apache.spark.ml.h2o.param

import ai.h2o.sparkling.macros.DeprecatedMethod
import hex.tree.SharedTreeModel.SharedTreeParameters
import hex.tree.SharedTreeModel.SharedTreeParameters.HistogramType
import org.apache.spark.ml.h2o.param.H2OAlgoParamsHelper.getValidatedEnumValue

trait H2OSharedTreeParams[P <: SharedTreeParameters] extends H2OAlgoParams[P] {

  //
  // Param definitions
  //
  final val ntrees = intParam("ntrees")
  final val maxDepth = intParam("maxDepth")
  final val minRows = doubleParam("minRows")
  final val nbins = intParam("nbins")
  final val nbinsCats = intParam("nbinsCats")
  final val minSplitImprovement = doubleParam("minSplitImprovement")
  final val histogramType = stringParam("histogramType")
  final val r2Stopping = doubleParam("r2Stopping")
  final val nbinsTopLevel = intParam("nbinsTopLevel")
  final val buildTreeOneNode = booleanParam("buildTreeOneNode")
  final val scoreTreeInterval = intParam("scoreTreeInterval")
  final val sampleRate = doubleParam("sampleRate")
  final val sampleRatePerClass =  nullableDoubleArrayParam("sampleRatePerClass")
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
    r2Stopping -> parameters._r2_stopping,
    nbinsTopLevel -> parameters._nbins_top_level,
    buildTreeOneNode -> parameters._build_tree_one_node,
    scoreTreeInterval -> parameters._score_tree_interval,
    sampleRate -> parameters._sample_rate,
    sampleRatePerClass -> parameters._sample_rate_per_class,
    colSampleRateChangePerLevel -> parameters._col_sample_rate_change_per_level,
    colSampleRatePerTree -> parameters._col_sample_rate_per_tree
  )

  //
  // Getters
  //
  def getNtrees() = $(ntrees)

  def getMaxDepth() = $(maxDepth)

  def getMinRows() = $(minRows)

  def getNbins() = $(nbins)

  def getNbinsCats() = $(nbinsCats)

  def getMinSplitImprovement() = $(minSplitImprovement)

  def getHistogramType() = $(histogramType)

  def getR2Stopping() = $(r2Stopping)

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

  @DeprecatedMethod("setHistogramType(value: String)")
  def setHistogramType(value: HistogramType): this.type = setHistogramType(value.name())

  def setHistogramType(value: String): this.type = {
    val validated = getValidatedEnumValue[HistogramType](value)
    set(histogramType, validated)
  }

  def setR2Stopping(value: Double): this.type = set(r2Stopping, value)

  def setNbinsTopLevel(value: Int): this.type = set(nbinsTopLevel, value)

  def setBuildTreeOneNode(value: Boolean): this.type = set(buildTreeOneNode, value)

  def setScoreTreeInterval(value: Int): this.type = set(scoreTreeInterval, value)

  def setSampleRate(value: Double): this.type = set(sampleRate, value)

  def setSampleRatePerClass(value: Array[Double]): this.type = set(sampleRatePerClass, value)

  def setColSampleRateChangePerLevel(value: Double): this.type = set(colSampleRateChangePerLevel, value)

  def setColSampleRatePerTree(value: Double): this.type = set(colSampleRatePerTree, value)

  override def updateH2OParams(): Unit = {
    super.updateH2OParams()
    parameters._ntrees = $(ntrees)
    parameters._max_depth = $(maxDepth)
    parameters._min_rows = $(minRows)
    parameters._nbins = $(nbins)
    parameters._nbins_cats = $(nbinsCats)
    parameters._min_split_improvement = $(minSplitImprovement)
    parameters._histogram_type = HistogramType.valueOf($(histogramType))
    parameters._r2_stopping = $(r2Stopping)
    parameters._nbins_top_level = $(nbinsTopLevel)
    parameters._build_tree_one_node = $(buildTreeOneNode)
    parameters._score_tree_interval = $(scoreTreeInterval)
    parameters._sample_rate = $(sampleRate)
    parameters._sample_rate_per_class = $(sampleRatePerClass)
    parameters._col_sample_rate_change_per_level = $(colSampleRateChangePerLevel)
    parameters._col_sample_rate_per_tree = $(colSampleRatePerTree)
  }
}
