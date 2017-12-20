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

import hex.tree.SharedTreeModel.SharedTreeParameters
import hex.tree.SharedTreeModel.SharedTreeParameters.HistogramType
import org.apache.spark.ml.param.{Param, ParamPair, Params}
import org.json4s.{JNull, JString, JValue}
import org.json4s.jackson.JsonMethods.{compact, parse, render}

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
  final val histogramType = H2OHistogramTypeParam("histogramType")
  final val r2Stopping = doubleParam("r2Stopping")
  final val nbinsTopLevel = intParam("nbinsTopLevel")
  final val buildTreeOneNode = booleanParam("buildTreeOneNode")
  final val scoreTreeInterval = intParam("scoreTreeInterval")
  final val sampleRate = doubleParam("sampleRate")
  final val sampleRatePerClass = doubleArrayParam("sampleRatePerClass")
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
    histogramType -> parameters._histogram_type,
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
  /** @group getParam */
  def getNtrees() = $(ntrees)

  /** @group getParam */
  def getMaxDepth() = $(maxDepth)

  /** @group getParam */
  def getMinRows() = $(minRows)

  /** @group getParam */
  def getNbins() = $(nbins)

  /** @group getParam */
  def getNbinsCats() = $(nbinsCats)

  /** @group getParam */
  def getMinSplitImprovement() = $(minSplitImprovement)

  /** @group getParam */
  def getHistogramType() = $(histogramType)

  /** @group getParam */
  def getR2Stopping() = $(r2Stopping)

  /** @group getParam */
  def getNbinsTopLevel() = $(nbinsTopLevel)

  /** @group getParam */
  def getBuildTreeOneNode() = $(buildTreeOneNode)

  /** @group getParam */
  def getScoreTreeInterval() = $(scoreTreeInterval)

  /** @group getParam */
  def getSampleRate() = $(sampleRate)

  /** @group getParam */
  def getSampleRatePerClass() = $(sampleRatePerClass)

  /** @group getParam */
  def getColSampleRateChangePerLevel() = $(colSampleRateChangePerLevel)

  /** @group getParam */
  def getColSampleRatePerTree() = $(colSampleRatePerTree)

  //
  // Setters
  //
  /** @group setParam */
  def setNtrees(value: Int): this.type = set(ntrees, value)

  /** @group setParam */
  def setMaxDepth(value: Int): this.type = set(maxDepth, value)

  /** @group setParam */
  def setMinRows(value: Double): this.type = set(minRows, value)

  /** @group setParam */
  def setNbins(value: Int): this.type = set(nbins, value)

  /** @group setParam */
  def setNbinsCats(value: Int): this.type = set(nbinsCats, value)

  /** @group setParam */
  def setMinSplitImprovement(value: Double): this.type = set(minSplitImprovement, value)

  /** @group setParam */
  def setHistogramType(value: HistogramType): this.type = set(histogramType, value)

  /** @group setParam */
  def setR2Stopping(value: Double): this.type = set(r2Stopping, value)

  /** @group setParam */
  def setNbinsTopLevel(value: Int): this.type = set(nbinsTopLevel, value)

  /** @group setParam */
  def setBuildTreeOneNode(value: Boolean): this.type = set(buildTreeOneNode, value)

  /** @group setParam */
  def setScoreTreeInterval(value: Int): this.type = set(scoreTreeInterval, value)

  /** @group setParam */
  def setSampleRate(value: Double): this.type = set(sampleRate, value)

  /** @group setParam */
  def setSampleRatePerClass(value: Array[Double]): this.type = set(sampleRatePerClass, value)

  /** @group setParam */
  def setColSampleRateChangePerLevel(value: Double): this.type = set(colSampleRateChangePerLevel, value)

  /** @group setParam */
  def setColSampleRatePerTree(value: Double): this.type = set(colSampleRatePerTree, value)


  def H2OHistogramTypeParam(name: String): H2OHistogramTypeParam = {
    new H2OHistogramTypeParam(this, name, getDoc(None, name))
  }

  override def updateH2OParams(): Unit = {
    super.updateH2OParams()
    parameters._ntrees = $(ntrees)
    parameters._max_depth = $(maxDepth)
    parameters._min_rows = $(minRows)
    parameters._nbins = $(nbins)
    parameters._nbins_cats = $(nbinsCats)
    parameters._min_split_improvement = $(minSplitImprovement)
    parameters._histogram_type = $(histogramType)
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

class H2OHistogramTypeParam private(parent: Params, name: String, doc: String, isValid: HistogramType => Boolean)
  extends Param[HistogramType](parent, name, doc, isValid) {

  def this(parent: Params, name: String, doc: String) = this(parent, name, doc, _ => true)

  /** Creates a param pair with the given value (for Java). */
  override def w(value: HistogramType): ParamPair[HistogramType] = super.w(value)

  override def jsonEncode(value: HistogramType): String = {
    val encoded: JValue = if (value == null) {
      JNull
    } else {
      JString(value.toString)
    }
    compact(render(encoded))
  }

  override def jsonDecode(json: String): HistogramType = {
    val parsed = parse(json)
    parsed match {
      case JString(x) =>
        HistogramType.valueOf(x)
      case JNull =>
        null
      case _ =>
        throw new IllegalArgumentException(s"Cannot decode $parsed to HistogramType.")
    }

  }
}
