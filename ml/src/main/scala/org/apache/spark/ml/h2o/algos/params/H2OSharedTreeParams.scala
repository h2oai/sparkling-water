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
package org.apache.spark.ml.h2o.algos.params

import hex.tree.SharedTreeModel.SharedTreeParameters

trait H2OSharedTreeParams[P <: SharedTreeParameters] extends H2OAlgoParams[P] {

  final val ntrees = intParam("ntrees")
  final val maxDepth = intParam("maxDepth")
  final val minRows = doubleParam("minRows")
  final val nbins = intParam("nbins")
  final val nbinsCat = intParam("nbinsCats")
  final val minSplitImprovement = doubleParam("minSplitImprovement")
  final val r2Stopping = doubleParam("r2Stopping")
  final val seed = longParam("seed")
  final val nbinsTopLevel = intParam("nbinsTopLevel")
  final val buildTreeOneNode = booleanParam("buildTreeOneNode")
  final val scoreTreeInterval = intParam("scoreTreeInterval")
  final val sampleRate = doubleParam("sampleRate")

  setDefault(
    ntrees -> parameters._ntrees,
    maxDepth -> parameters._max_depth,
    minRows -> parameters._min_rows,
    nbins -> parameters._nbins,
    nbinsCat -> parameters._nbins_cats,
    minSplitImprovement -> parameters._min_split_improvement,
    r2Stopping -> parameters._r2_stopping,
    seed -> parameters._seed,
    nbinsTopLevel -> parameters._nbins_top_level,
    buildTreeOneNode -> parameters._build_tree_one_node,
    scoreTreeInterval -> parameters._score_tree_interval,
    sampleRate -> parameters._sample_rate)
}
